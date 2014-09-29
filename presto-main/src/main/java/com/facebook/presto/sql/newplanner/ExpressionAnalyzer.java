/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.newplanner;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.analyzer.TupleDescriptor;
import com.facebook.presto.sql.newplanner.expression.RelationalExpression;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Extract;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NegativeExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.google.common.collect.Iterables;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static com.facebook.presto.spi.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.AMBIGUOUS_ATTRIBUTE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_ATTRIBUTE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MULTIPLE_FIELDS_FROM_SCALAR_SUBQUERY;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.facebook.presto.util.DateTimeUtils.timeHasTimeZone;
import static com.facebook.presto.util.DateTimeUtils.timestampHasTimeZone;
import static com.google.common.base.Preconditions.checkArgument;

public class ExpressionAnalyzer
{
    private final Map<Expression, Type> types = new HashMap<>();
    private final Map<SubqueryExpression, RelationalExpression> translations = new HashMap<>();
    private final Set<Expression> aggregations = new HashSet<>();
    private final Map<QualifiedNameReference, ResolvedName> resolvedNames = new HashMap<>();

    private final Scope scope;
    private final SqlToRelationalTranslator queryTranslator;

    public ExpressionAnalyzer(Scope scope, SqlToRelationalTranslator queryTranslator)
    {
        this.scope = scope;
        this.queryTranslator = queryTranslator;
    }

    public void analyze(Expression expression)
    {
        expression.accept(new Visitor(), null);
    }

    public ExpressionAnalysis getResult()
    {
        return new ExpressionAnalysis(types, aggregations, translations, null); // TODO: coercions
    }

    private class Visitor
            extends AstVisitor<Type, Void>
    {
        public Type process(Node node)
        {
            return process(node, null);
        }

        @Override
        public Type process(Node node, Void context)
        {
            checkArgument(node instanceof Expression, "node is not an Expression, actual %s", node.getClass().getName());

            // don't re-process a node
            Type type = types.get(node);
            if (type != null) {
                return type;
            }

            return super.process(node, context);
        }

        @Override
        protected Type visitCurrentTime(CurrentTime node, Void context)
        {
            if (node.getPrecision() != null) {
                throw new SemanticException(NOT_SUPPORTED, node, "non-default precision not yet supported");
            }

            Type type;
            switch (node.getType()) {
                case DATE:
                    type = DATE;
                    break;
                case TIME:
                    type = TIME_WITH_TIME_ZONE;
                    break;
                case LOCALTIME:
                    type = TIME;
                    break;
                case TIMESTAMP:
                    type = TIMESTAMP_WITH_TIME_ZONE;
                    break;
                case LOCALTIMESTAMP:
                    type = TIMESTAMP;
                    break;
                default:
                    throw new SemanticException(NOT_SUPPORTED, node, "%s not yet supported", node.getType().getName());
            }

            types.put(node, type);
            return type;
        }

        @Override
        protected Type visitNotExpression(NotExpression node, Void context)
        {
            // TODO: coercion
            validateType(node, process(node.getValue()), BOOLEAN, "Value of logical NOT expression");
            types.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            // TODO: coercion
            validateType(node, process(node.getLeft()), BOOLEAN, "Left side of logical expression");
            validateType(node, process(node.getRight()), BOOLEAN, "Right side of logical expression");

            types.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitComparisonExpression(ComparisonExpression node, Void context)
        {
//            OperatorType operatorType;
//            if (node.getType() == ComparisonExpression.Type.IS_DISTINCT_FROM) {
//                operatorType = OperatorType.EQUAL;
//            }
//            else {
//                operatorType = OperatorType.valueOf(node.getType().name());
//            }
//
//            return getOperator(context, node, operatorType, node.getLeft(), node.getRight());
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        protected Type visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            process(node.getValue());
            types.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
            process(node.getValue());
            types.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitNullIfExpression(NullIfExpression node, Void context)
        {
            Type firstType = process(node.getFirst());
            Type secondType = process(node.getSecond());

            if (!firstType.equals(secondType)) {
                // TODO: coercion
                throw new SemanticException(TYPE_MISMATCH, node, "Types are not comparable with NULLIF: %s vs %s", firstType, secondType);
            }

            types.put(node, firstType);
            return firstType;
        }

        @Override
        protected Type visitIfExpression(IfExpression node, Void context)
        {
            // TODO: coercion
            validateType(node.getCondition(), process(node.getCondition()), BOOLEAN, "IF condition");

            Type type = process(node.getTrueValue());
            if (node.getFalseValue().isPresent()) {
                Type falseType = process(node.getFalseValue().get());
                if (!type.equals(falseType)) {
                    throw new SemanticException(TYPE_MISMATCH, node, "Result types for IF must be the same: %s vs %s", type, falseType);
                }
            }

            types.put(node, type);
            return type;
        }

        @Override
        protected Type visitSearchedCaseExpression(SearchedCaseExpression node, Void context)
        {
//            for (WhenClause whenClause : node.getWhenClauses()) {
//                coerceType(context, whenClause.getOperand(), BOOLEAN, "CASE WHEN clause");
//            }
//
//            Type type = coerceToSingleType(context,
//                    "All CASE results must be the same type: %s",
//                    getCaseResultExpressions(node.getWhenClauses(), node.getDefaultValue()));
//            expressionTypes.put(node, type);
//
//            for (WhenClause whenClause : node.getWhenClauses()) {
//                Type whenClauseType = process(whenClause.getResult(), context);
//                checkNotNull(whenClauseType, "Expression types does not contain an entry for %s", whenClause);
//                expressionTypes.put(whenClause, whenClauseType);
//            }
//
//            return type;
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        protected Type visitSimpleCaseExpression(SimpleCaseExpression node, Void context)
        {
//            for (WhenClause whenClause : node.getWhenClauses()) {
//                coerceToSingleType(context, node, "CASE operand type does not match WHEN clause operand type: %s vs %s", node.getOperand(), whenClause.getOperand());
//            }
//
//            Type type = coerceToSingleType(context,
//                    "All CASE results must be the same type: %s",
//                    getCaseResultExpressions(node.getWhenClauses(), node.getDefaultValue()));
//            expressionTypes.put(node, type);
//
//            for (WhenClause whenClause : node.getWhenClauses()) {
//                Type whenClauseType = process(whenClause.getResult(), context);
//                checkNotNull(whenClauseType, "Expression types does not contain an entry for %s", whenClause);
//                expressionTypes.put(whenClause, whenClauseType);
//            }
//
//            return type;
            throw new UnsupportedOperationException("not yet implemented");
        }

//        private List<Expression> getCaseResultExpressions(List<WhenClause> whenClauses, Expression defaultValue)
//        {
//            List<Expression> resultExpressions = new ArrayList<>();
//            for (WhenClause whenClause : whenClauses) {
//                resultExpressions.add(whenClause.getResult());
//            }
//            if (defaultValue != null) {
//                resultExpressions.add(defaultValue);
//            }
//            return resultExpressions;
//        }

        @Override
        protected Type visitCoalesceExpression(CoalesceExpression node, Void context)
        {
//            Type type = coerceToSingleType(context, "All COALESCE operands must be the same type: %s", node.getOperands());
//
//            expressionTypes.put(node, type);
//            return type;
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        protected Type visitNegativeExpression(NegativeExpression node, Void context)
        {
//            return getOperator(context, node, OperatorType.NEGATION, node.getValue());
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        protected Type visitArithmeticExpression(ArithmeticExpression node, Void context)
        {
//            return getOperator(context, node, OperatorType.valueOf(node.getType().name()), node.getLeft(), node.getRight());
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        protected Type visitLikePredicate(LikePredicate node, Void context)
        {
            validateType(node, process(node.getValue()), VARCHAR, "Left side of LIKE expression");
            validateType(node, process(node.getPattern()), VARCHAR, "Pattern for LIKE expression");
            if (node.getEscape() != null) {
                validateType(node, process(node.getPattern()), VARCHAR, "Escape for LIKE expression");
            }

            types.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitSubscriptExpression(SubscriptExpression node, Void context)
        {
//            return getOperator(context, node, SUBSCRIPT, node.getBase(), node.getIndex());
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        protected Type visitArrayConstructor(ArrayConstructor node, Void context)
        {
//            Type type = coerceToSingleType(context, "All ARRAY elements must be the same type: %s", node.getValues());
//            Type arrayType = metadata.getTypeManager().getParameterizedType(ARRAY.getName(), ImmutableList.of(type.getName()));
//            expressionTypes.put(node, arrayType);
//            return arrayType;
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        protected Type visitStringLiteral(StringLiteral node, Void context)
        {
            types.put(node, VARCHAR);
            return VARCHAR;
        }

        @Override
        protected Type visitLongLiteral(LongLiteral node, Void context)
        {
            types.put(node, BIGINT);
            return BIGINT;
        }

        @Override
        protected Type visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            types.put(node, DOUBLE);
            return DOUBLE;
        }

        @Override
        protected Type visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            types.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitGenericLiteral(GenericLiteral node, Void context)
        {
//            Type type = metadata.getType(node.getType());
//            if (type == null) {
//                throw new SemanticException(TYPE_MISMATCH, node, "Unknown type: " + node.getType());
//            }
//
//            try {
//                metadata.getFunctionRegistry().getCoercion(VARCHAR, type);
//            }
//            catch (IllegalArgumentException e) {
//                throw new SemanticException(TYPE_MISMATCH, node, "No literal form for type %s", type);
//            }
//
//            expressionTypes.put(node, type);
//            return type;
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        protected Type visitTimeLiteral(TimeLiteral node, Void context)
        {
            Type type;
            if (timeHasTimeZone(node.getValue())) {
                type = TIME_WITH_TIME_ZONE;
            }
            else {
                type = TIME;
            }
            types.put(node, type);
            return type;
        }

        @Override
        protected Type visitTimestampLiteral(TimestampLiteral node, Void context)
        {
            Type type;
            if (timestampHasTimeZone(node.getValue())) {
                type = TIMESTAMP_WITH_TIME_ZONE;
            }
            else {
                type = TIMESTAMP;
            }
            types.put(node, type);
            return type;
        }

        @Override
        protected Type visitIntervalLiteral(IntervalLiteral node, Void context)
        {
            Type type;
            if (node.isYearToMonth()) {
                type = INTERVAL_YEAR_MONTH;
            }
            else {
                type = INTERVAL_DAY_TIME;
            }
            types.put(node, type);
            return type;
        }

        @Override
        protected Type visitNullLiteral(NullLiteral node, Void context)
        {
            types.put(node, UNKNOWN);
            return UNKNOWN;
        }

        @Override
        protected Type visitFunctionCall(FunctionCall node, Void context)
        {
//            if (node.getWindow().isPresent()) {
//                for (Expression expression : node.getWindow().get().getPartitionBy()) {
//                    process(expression, context);
//                }
//
//                for (SortItem sortItem : node.getWindow().get().getOrderBy()) {
//                    process(sortItem.getSortKey(), context);
//                }
//            }
//
//            ImmutableList.Builder<String> argumentTypes = ImmutableList.builder();
//            for (Expression expression : node.getArguments()) {
//                argumentTypes.add(process(expression, context).getName());
//            }
//
//            FunctionInfo function = metadata.resolveFunction(node.getName(), argumentTypes.build(), context.isApproximate());
//            for (int i = 0; i < node.getArguments().size(); i++) {
//                Expression expression = node.getArguments().get(i);
//                Type type = metadata.getType(function.getArgumentTypes().get(i));
//                checkNotNull(type, "Type %s not found", function.getArgumentTypes().get(i));
//                coerceType(context, expression, type, String.format("Function %s argument %d", function.getSignature(), i));
//            }
//            resolvedFunctions.put(node, function);
//
//            Type type = metadata.getType(function.getReturnType());
//            expressionTypes.put(node, type);
//
//            return type;
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        protected Type visitExtract(Extract node, Void context)
        {
//            Type type = process(node.getExpression(), context);
//            if (!isDateTimeType(type)) {
//                throw new SemanticException(TYPE_MISMATCH, node.getExpression(), "Type of argument to extract must be DATE, TIME, TIMESTAMP, or INTERVAL (actual %s)", type);
//            }
//            Extract.Field field = node.getField();
//            if ((field == TIMEZONE_HOUR || field == TIMEZONE_MINUTE) && !(type == TIME_WITH_TIME_ZONE || type == TIMESTAMP_WITH_TIME_ZONE)) {
//                throw new SemanticException(TYPE_MISMATCH, node.getExpression(), "Type of argument to extract time zone field must have a time zone (actual %s)", type);
//            }
//
//            expressionTypes.put(node, BIGINT);
//            return BIGINT;
            throw new UnsupportedOperationException("not yet implemented");
        }

//        private boolean isDateTimeType(Type type)
//        {
//            return type == DATE ||
//                    type == TIME ||
//                    type == TIME_WITH_TIME_ZONE ||
//                    type == TIMESTAMP ||
//                    type == TIMESTAMP_WITH_TIME_ZONE ||
//                    type == INTERVAL_DAY_TIME ||
//                    type == INTERVAL_YEAR_MONTH;
//        }

        @Override
        protected Type visitBetweenPredicate(BetweenPredicate node, Void context)
        {
//            return getOperator(context, node, OperatorType.BETWEEN, node.getValue(), node.getMin(), node.getMax());
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        public Type visitCast(Cast node, Void context)
        {
//            Type type = metadata.getType(node.getType());
//            if (type == null) {
//                throw new SemanticException(TYPE_MISMATCH, node, "Unknown type: " + node.getType());
//            }
//
//            Type value = process(node.getExpression(), context);
//            if (value != UNKNOWN) {
//                try {
//                    metadata.getFunctionRegistry().getCoercion(value, type);
//                }
//                catch (OperatorNotFoundException e) {
//                    throw new SemanticException(TYPE_MISMATCH, node, "Cannot cast %s to %s", value, type);
//                }
//            }
//
//            expressionTypes.put(node, type);
//            return type;
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        protected Type visitInPredicate(InPredicate node, Void context)
        {
//            Expression value = node.getValue();
//            process(value, context);
//
//            Expression valueList = node.getValueList();
//            process(valueList, context);
//
//            if (valueList instanceof InListExpression) {
//                InListExpression inListExpression = (InListExpression) valueList;
//
//                coerceToSingleType(context,
//                        "IN value and list items must be the same type: %s",
//                        ImmutableList.<Expression>builder().add(value).addAll(inListExpression.getValues()).build());
//            }
//            else if (valueList instanceof SubqueryExpression) {
//                subqueryInPredicates.add(node);
//            }
//
//            expressionTypes.put(node, BOOLEAN);
//            return BOOLEAN;
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        protected Type visitInListExpression(InListExpression node, Void context)
        {
//            Type type = coerceToSingleType(context, "All IN list values must be the same type: %s", node.getValues());
//
//            expressionTypes.put(node, type);
//            return type; // TODO: this really should a be relation type
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        protected Type visitQualifiedNameReference(QualifiedNameReference node, Void context)
        {
            for (Scope scope = ExpressionAnalyzer.this.scope; scope != null; scope = scope.getParent()) {
                TupleDescriptor rowType = scope.getTupleDescriptor();

                List<Field> matches = rowType.resolveFields(node.getName());
                if (matches.size() > 1) {
                    throw new SemanticException(AMBIGUOUS_ATTRIBUTE, node, "Column '%s' is ambiguous", node.getName());
                }
                else if (matches.size() == 1) {
                    Field field = Iterables.getOnlyElement(matches);
                    int fieldIndex = rowType.indexOf(field);
                    types.put(node, field.getType());
                    resolvedNames.put(node, new ResolvedName(scope, fieldIndex));

                    return null;
                }
            }

            throw new SemanticException(MISSING_ATTRIBUTE, node, "Column '%s' cannot be resolved", node.getName());
        }

        @Override
        protected Type visitSubqueryExpression(SubqueryExpression node, Void context)
        {
            TranslatedRelationalExpression translated = queryTranslator.translate(node.getQuery(), scope);

            TupleDescriptor rowType = translated.getFields();

            // Scalar subqueries should only produce one column TODO: support tuple types
            if (rowType.getVisibleFieldCount() != 1) {
                throw new SemanticException(MULTIPLE_FIELDS_FROM_SCALAR_SUBQUERY,
                        node,
                        "Subquery expression must produce only one field. Found %s",
                        rowType.getVisibleFieldCount());
            }

            Type type = Iterables.getOnlyElement(rowType.getVisibleFields()).getType();

            types.put(node, type);
            translations.put(node, translated.getExpression());

            return type;
        }

        private void validateType(Expression expression, Type actualType, Type expectedType, String message)
        {
            if (!actualType.equals(expectedType)) {
                throw new SemanticException(TYPE_MISMATCH, expression, message + " must evaluate to a %s (actual: %s)", expectedType, actualType);
            }
        }
    }
}
