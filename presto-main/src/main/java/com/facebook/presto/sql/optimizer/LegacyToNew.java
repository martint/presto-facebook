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
package com.facebook.presto.sql.optimizer;

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.optimizer.tree.Expression;
import com.facebook.presto.sql.optimizer.tree.Lambda;
import com.facebook.presto.sql.optimizer.tree.Value;
import com.facebook.presto.sql.optimizer.tree.sql.Null;
import com.facebook.presto.sql.optimizer.tree.type.LambdaTypeStamp;
import com.facebook.presto.sql.optimizer.tree.type.RelationTypeStamp;
import com.facebook.presto.sql.optimizer.tree.type.RowTypeStamp;
import com.facebook.presto.sql.optimizer.tree.type.TypeStamp;
import com.facebook.presto.sql.optimizer.tree.type.UnknownTypeStamp;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExceptNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.IntersectNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SetOperationNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.AtTimeZone;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BinaryLiteral;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Extract;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.TryExpression;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.type.UnknownType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.optimizer.tree.Expressions.call;
import static com.facebook.presto.sql.optimizer.tree.Expressions.fieldDereference;
import static com.facebook.presto.sql.optimizer.tree.Expressions.lambda;
import static com.facebook.presto.sql.optimizer.tree.Expressions.localReference;
import static com.facebook.presto.sql.optimizer.tree.Expressions.reference;
import static com.facebook.presto.sql.optimizer.tree.Expressions.value;
import static com.facebook.presto.sql.optimizer.utils.CollectionConstructors.list;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class LegacyToNew
{
    private final Map<Symbol, Type> types;

    public LegacyToNew(Map<Symbol, Type> types)
    {
        this.types = ImmutableMap.copyOf(types);
    }

    public Expression translate(PlanNode node)
    {
        return translate(node, new Scope());
    }

    private Expression translate(PlanNode node, Scope scope)
    {
        return node.accept(new PlanTranslator(types), scope);
    }

    private Expression translate(com.facebook.presto.sql.tree.Expression expression, Scope scope)
    {
        return new ExpressionTranslator().process(expression, scope);
    }

    private static class Scope
    {
        private final Optional<Scope> parent;
        private final Resolver resolver;

        public Scope()
        {
            this.parent = Optional.empty();
            this.resolver = (name, localVariable) -> Optional.empty();
        }

        private Scope(Optional<Scope> parent, Resolver resolver)
        {
            this.parent = parent;
            this.resolver = resolver;
        }

        public Scope(Resolver resolver)
        {
            this(Optional.empty(), resolver);
        }

        public Scope(Scope parent, Resolver resolver)
        {
            this(Optional.of(parent), resolver);
        }
    }

    private interface Resolver
    {
        Optional<Expression> resolve(String name, Function<TypeStamp, Expression> localVariable);
    }

    Resolver forSymbols(List<Symbol> symbols)
    {
        List<Type> columnTypes = symbols.stream()
                .map(types::get)
                .collect(toList());

        List<String> names = symbols.stream()
                .map(Symbol::getName)
                .collect(toList());

        return forNames(names, columnTypes);
    }

    static Resolver forNames(List<String> names, List<Type> columnTypes)
    {
        return (name, localVariable) -> {
            if (names.contains(name)) {
                int index = names.indexOf(name);
                TypeStamp type = translate(columnTypes.get(index));
                return Optional.of(fieldDereference(translate(columnTypes.get(index)), localVariable.apply(type), index));
            }

            return Optional.empty();
        };
    }

    private static TypeStamp translate(Type type)
    {
        requireNonNull(type, "type is null");

        if (type.equals(UnknownType.UNKNOWN)) {
            return new UnknownTypeStamp();
        }

        throw new UnsupportedOperationException("not yet implemented: " + type);
    }

    private class PlanTranslator
            extends PlanVisitor<Scope, Expression>
    {
        private final Map<Symbol, Type> types;

        public PlanTranslator(Map<Symbol, Type> types)
        {
            this.types = types;
        }

        private TypeStamp typeOf(PlanNode node)
        {
            List<TypeStamp> columnTypes = node.getOutputSymbols().stream()
                    .map(types::get)
                    .map(LegacyToNew::translate)
                    .collect(toList());

            return new RelationTypeStamp(new RowTypeStamp(columnTypes));
        }

        @Override
        protected Expression visitPlan(PlanNode node, Scope scope)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        @Override
        public Expression visitEnforceSingleRow(EnforceSingleRowNode node, Scope scope)
        {
            return call(typeOf(node), "enforce-single-row", translate(node.getSource(), scope));
        }

        @Override
        public Expression visitLimit(LimitNode node, Scope scope)
        {
            return call(typeOf(node), "limit", translate(node.getSource(), scope), value(translate(BigintType.BIGINT), node.getCount()));
        }

        @Override
        public Expression visitOutput(OutputNode node, Scope scope)
        {
            List<Expression> expressions = node.getOutputSymbols().stream()
                    .map(output -> translate(output.toSymbolReference(), new Scope(scope, forSymbols(node.getSource().getOutputSymbols()))))
                    .collect(toList());

            RowTypeStamp rowType = new RowTypeStamp(expressions.stream().map(Expression::type).collect(toList()));
            Expression source = translate(node.getSource(), scope);
            return call(
                    typeOf(node),
                    "transform",
                    source,
                    lambda(
                            new LambdaTypeStamp(((RelationTypeStamp) source.type()).getRowType(), rowType),
                            call(rowType, "row", expressions)));
        }

        @Override
        public Expression visitFilter(FilterNode node, Scope scope)
        {
            Expression lambdaBody = translate(node.getPredicate(), new Scope(scope, forSymbols(node.getSource().getOutputSymbols())));

            return call(typeOf(node), "logical-filter", translate(node.getSource(), scope), lambda(new LambdaTypeStamp(null, null), lambdaBody)); // TODO: lambda type
        }

        @Override
        public Expression visitProject(ProjectNode node, Scope scope)
        {
            List<Expression> projections = node.getOutputSymbols().stream()
                    .map(output -> {
                        List<Symbol> outputSymbols = node.getSource().getOutputSymbols();
                        return translate(node.getAssignments().get(output), new Scope(scope, forSymbols(outputSymbols)));
                    })
                    .collect(toList());

            RowTypeStamp rowType = new RowTypeStamp(projections.stream().map(Expression::type).collect(toList()));
            Expression source = translate(node.getSource(), scope);
            return call(
                    typeOf(node),
                    "transform",
                    source,
                    lambda(
                            new LambdaTypeStamp(((RelationTypeStamp) source.type()).getRowType(), rowType),
                            call(
                                    rowType,
                                    "row",
                                    projections)));
        }

        @Override
        public Expression visitValues(ValuesNode node, Scope scope)
        {
            return call(
                    typeOf(node),
                    "array", node.getRows().stream()
                            .map(row -> {
                                List<Expression> items = row.stream()
                                        .map(column -> translate(column, scope))
                                        .collect(toImmutableList());

                                return call(
                                        new RowTypeStamp(items.stream().map(Expression::type).collect(toList())),
                                        "row",
                                        items);
                            })
                            .collect(toImmutableList()));
        }

        @Override
        public Expression visitSort(SortNode node, Scope scope)
        {
            List<Expression> criteria = node.getOrderBy().stream()
                    .map(input ->
                            call(
                                    null, // TODO: row type
                                    "row",
                                    lambda(
                                            new LambdaTypeStamp(null, null), // TODO: lambda type
                                            translate(input.toSymbolReference(), new Scope(scope, forSymbols(node.getSource().getOutputSymbols())))),
                                    value(null, // TODO
                                            node.getOrderings().get(input))))
                    .collect(toImmutableList());

            return call(
                    typeOf(node),
                    "sort",
                    translate(node.getSource(), scope),
                    call(null, "array", criteria)); // TODO: type
        }

        @Override
        public Expression visitTopN(TopNNode node, Scope scope)
        {
            List<Expression> criteria = node.getOrderBy().stream()
                    .map(input ->
                            call(
                                    null, // TODO: row type
                                    "row",
                                    lambda(
                                            new LambdaTypeStamp(null, null), // TODO: lambda type
                                            translate(input.toSymbolReference(), new Scope(scope, forSymbols(node.getSource().getOutputSymbols())))),
                                    value(null, node.getOrderings().get(input)))) // TODO: type
                    .collect(toImmutableList());

            return call(
                    typeOf(node),
                    "top-n",
                    translate(node.getSource(), scope),
                    value(null, node.getCount()), // TODO: type
                    call(null, "array", criteria)); // TODO: type
        }

        @Override
        public Expression visitSample(SampleNode node, Scope scope)
        {
            return call(
                    typeOf(node),
                    "sample",
                    translate(node.getSource(), scope),
                    value(null, node.getSampleType()),    // TODO: type
                    value(null, node.getSampleRatio()));  // TODO: type
        }

        @Override
        public Expression visitTableScan(TableScanNode node, Scope context)
        {
            // TODO: column ordering
            return call(
                    typeOf(node),
                    "table",
                    value(null, node.getTable())); // TODO: type
        }

        @Override
        public Expression visitAggregation(AggregationNode node, Scope scope)
        {
            checkArgument(node.getMasks().isEmpty(), "distinct aggregations not supported");
            checkArgument(!node.getHashSymbol().isPresent(), "pre-computed hash not supported");

            Expression source = translate(node.getSource(), scope);

            List<Expression> calls = node.getOutputSymbols().stream()
                    .filter(node.getAggregations()::containsKey)
                    .map(output -> lambda(
                            new LambdaTypeStamp(null, null), // TODO: lambda type
                            translate(node.getAggregations().get(output), new Scope(scope, forSymbols(node.getSource().getOutputSymbols())))))
                    .collect(toList());

            if (node.getGroupIdSymbol().isPresent()) {
                List<List<Value>> groupingSets = node.getGroupingSets().stream()
                        .map(set ->
                                set.stream()
                                        .map(column -> value(null, node.getSource().getOutputSymbols().indexOf(column))) // TODO: type
                                        .collect(toList()))
                        .collect(toList());

                // TODO functions, mask, etc
                return call(
                        typeOf(node),
                        "grouping-sets",
                        source,
                        call(
                                null, // TODO: type
                                "array",
                                value(null, groupingSets)), // TODO: type
                        value(null, node.getSource().getOutputSymbols().indexOf(node.getGroupIdSymbol().get()))); // TODO: type
            }
            else {
                // TODO functions, mask, etc
                return call(
                        typeOf(node),
                        "aggregation",
                        source,
                        call(null, "array", calls)); // TODO: type
            }
        }

        @Override
        public Expression visitJoin(JoinNode node, Scope parentScope)
        {
            checkArgument(!node.getLeftHashSymbol().isPresent(), "pre-computed hash not supported");
            checkArgument(!node.getRightHashSymbol().isPresent(), "pre-computed hash not supported");

            Expression left = translate(node.getLeft(), parentScope);
            Expression right = translate(node.getRight(), parentScope);

            List<String> leftFields = names(node.getLeft().getOutputSymbols());
            List<Type> leftTypes = node.getLeft().getOutputSymbols().stream()
                    .map(types::get)
                    .collect(toList());

            List<String> rightFields = names(node.getRight().getOutputSymbols());
            List<Type> rightTypes = node.getRight().getOutputSymbols().stream()
                    .map(types::get)
                    .collect(toList());

            Scope scope = new Scope(parentScope, (name, localVariable) -> {
                int index = leftFields.indexOf(name);
                if (index != -1) {
                    return Optional.of(fieldDereference(
                            translate(leftTypes.get(index)),
                            fieldDereference(null, localVariable.apply(left.type()), 0),
                            index));
                }

                index = rightFields.indexOf(name);
                if (index != -1) {
                    return Optional.of(fieldDereference(
                            translate(rightTypes.get(index)),
                            fieldDereference(null, localVariable.apply(right.type()), 1),
                            index));
                }

                return Optional.empty();
            });

            Expression criteria = null;
            for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
                Expression term = call(
                        null, // TODO: type
                        ComparisonExpressionType.EQUAL.toString(),
                        translate(clause.getLeft().toSymbolReference(), scope),
                        translate(clause.getRight().toSymbolReference(), scope));
                if (criteria == null) {
                    criteria = term;
                }
                else {
                    criteria = call(null, "and", criteria, term); // TODO: type
                }
            }

            if (node.getFilter().isPresent()) {
                Expression term = translate(node.getFilter().get(), scope);
                if (criteria == null) {
                    criteria = term;
                }
                else {
                    criteria = call(null, "and", criteria, term); // TODO: type
                }
            }

            if (criteria == null) {
                criteria = lambda(
                        new LambdaTypeStamp(null, null), // TODO: lambda type
                        value(null, true)); // TODO: type
            }

            return call(
                    typeOf(node),
                    "join",
                    left,
                    right,
                    lambda(
                            new LambdaTypeStamp(null, null), // TODO: lambda type
                            criteria),
                    value(null, node.getType())); // TODO: type
        }

        @Override
        public Expression visitApply(ApplyNode node, Scope scope)
        {
            return call(
                    typeOf(node),
                    "apply",
                    translate(node.getInput(), scope),
                    translate(node.getSubquery(), new Scope(scope, forSymbols(node.getInput().getOutputSymbols()))));
        }

        @Override
        public Expression visitUnion(UnionNode node, Scope scope)
        {
            return call(typeOf(node), "concat", translateSetOperationChildren(node, scope));
        }

        @Override
        public Expression visitIntersect(IntersectNode node, Scope scope)
        {
            return call(typeOf(node), "intersect", translateSetOperationChildren(node, scope));
        }

        @Override
        public Expression visitExcept(ExceptNode node, Scope scope)
        {
            List<Expression> children = translateSetOperationChildren(node, scope);

            Expression left = children.get(0);
            Expression right;
            if (children.size() > 2) {
                right = call(typeOf(node), "concat", children.subList(1, children.size()));
            }
            else {
                right = children.get(1);
            }

            return call(typeOf(node), "except", left, right);
        }

        @Override
        public Expression visitWindow(WindowNode node, Scope scope)
        {
            checkArgument(!node.getHashSymbol().isPresent(), "pre-computed hash not supported");

            Expression source = translate(node.getSource(), scope);

            List<Expression> orderings = node.getOrderBy().stream()
                    .map(input ->
                            call(
                                    null, // TODO: type
                                    "row",
                                    lambda(
                                            new LambdaTypeStamp(null, null), // TODO: lambda type
                                            translate(input.toSymbolReference(), new Scope(scope, forSymbols(node.getSource().getOutputSymbols())))),
                                    value(null, node.getOrderings().get(input)))) // TODO: type
                    .collect(toImmutableList());

            List<Expression> partitionColumns = node.getPartitionBy().stream()
                    .map(column -> value(null, node.getSource().getOutputSymbols().indexOf(column))) // TODO: type
                    .collect(toList());

            // TODO: functions & frames
            return call(
                    typeOf(node),
                    "window",
                    source,
                    call(null, "array", partitionColumns), // TODO: type
                    call(null, "array", orderings));       // TODO: type
        }

        @Override
        public Expression visitUnnest(UnnestNode node, Scope scope)
        {
            checkArgument(!node.getOrdinalitySymbol().isPresent(), "unnest with ordinality not supported");
            checkArgument(node.getUnnestSymbols().values().stream().noneMatch(e -> e.size() > 1), "unnest with map columns not supported");

            // TODO: for MAP columns, translate as:
            //  (unnest
            //      <source>
            //      (array
            //          (lambda (map-keys (field %0 0)))
            //          (lambda (map-values (field %0 0)))))
            //

            List<Expression> columns = node.getUnnestSymbols().keySet().stream()
                    .map(e -> lambda(
                            new LambdaTypeStamp(null, null), // TODO: lambda type
                            fieldDereference(
                                    translate(types.get(e.getName())),
                                    localReference(null), // TODO: type
                                    node.getSource().getOutputSymbols().indexOf(e))))
                    .collect(toList());

            return call(
                    typeOf(node),
                    "unnest",
                    translate(node.getSource(), scope),
                    call(null, "array", columns)); // TODO: types
        }

        @Override
        public Expression visitGroupId(GroupIdNode node, Scope scope)
        {
            List<List<Value>> groupingSets = node.getGroupingSets().stream()
                    .map(set ->
                            set.stream()
                                    .map(column -> value(null, node.getSource().getOutputSymbols().indexOf(column))) // TODO: type
                                    .collect(toList()))
                    .collect(toList());

            return call(
                    typeOf(node),
                    "group-id",
                    translate(node.getSource(), scope),
                    value(null, groupingSets)); // TODO:type
        }

        @Override
        public Expression visitMarkDistinct(MarkDistinctNode node, Scope scope)
        {
            // TODO
            throw new UnsupportedOperationException("not yet implemented: markdistinct");
        }

        private List<Expression> translateSetOperationChildren(SetOperationNode node, Scope scope)
        {
            ImmutableList.Builder<Expression> children = ImmutableList.builder();

            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNode source = node.getSources().get(i);
                Map<Symbol, SymbolReference> assignments = node.sourceSymbolMap(i);

                children.add(
                        call(
                                null, // TODO: type
                                "transform",
                                translate(source, scope),
                                lambda(
                                        new LambdaTypeStamp(null, null), // TODO: lambda type
                                        call(
                                                null,  // TODO: type
                                                "row",
                                                node.getOutputSymbols().stream()
                                                        .map(output -> translate(assignments.get(output), new Scope(scope, forSymbols(source.getOutputSymbols()))))
                                                        .collect(toList())))));
            }

            return children.build();
        }
    }

    private static List<String> names(List<Symbol> symbols)
    {
        return symbols.stream()
                .map(Symbol::getName)
                .collect(toImmutableList());
    }

    private class ExpressionTranslator
            extends AstVisitor<Expression, Scope>
    {
        @Override
        protected Expression visitNode(Node node, Scope context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        @Override
        protected Expression visitRow(Row node, Scope scope)
        {
            return call(
                    null, // TODO: type
                    "row",
                    node.getItems().stream()
                            .map(item -> translate(item, scope))
                            .collect(toList()));
        }

        @Override
        protected Expression visitAtTimeZone(AtTimeZone node, Scope scope)
        {
            return call(
                    null, // TODO: type
                    "at_timezone",
                    translate(node.getValue(), scope),
                    translate(node.getTimeZone(), scope));
        }

        @Override
        protected Expression visitIntervalLiteral(IntervalLiteral node, Scope context)
        {
            return value(null, node); // TODO
        }

        @Override
        protected Expression visitDoubleLiteral(DoubleLiteral node, Scope context)
        {
            return value(null, node.getValue()); // TODO: type
        }

        @Override
        protected Expression visitBooleanLiteral(BooleanLiteral node, Scope context)
        {
            return value(null, node.getValue()); // TODO: type
        }

        @Override
        protected Expression visitStringLiteral(StringLiteral node, Scope context)
        {
            return value(null, node.getSlice()); // TODO: type
        }

        @Override
        protected Expression visitLongLiteral(LongLiteral node, Scope context)
        {
            return value(null, node.getValue()); // TODO: type
        }

        @Override
        protected Expression visitBinaryLiteral(BinaryLiteral node, Scope context)
        {
            return value(null, node.getValue()); // TODO: type
        }

        @Override
        protected Expression visitNullLiteral(NullLiteral node, Scope context)
        {
            return new Null(new UnknownTypeStamp()); // TODO: type
        }

        @Override
        protected Expression visitGenericLiteral(GenericLiteral node, Scope context)
        {
            return call(null, node.getType(), value(null, node.getValue())); // TODO: type
        }

        @Override
        protected Expression visitDereferenceExpression(DereferenceExpression node, Scope scope)
        {
            return fieldDereference(null, translate(node.getBase(), scope), node.getFieldName());
        }

        @Override
        protected Expression visitLambdaExpression(LambdaExpression node, Scope scope)
        {
            return lambda(
                    new LambdaTypeStamp(null, null), // TODO: lambda type
                    translate(node.getBody(), new Scope(scope, forNames(node.getArguments(), null)))); // TODO: types
        }

        @Override
        protected Expression visitTryExpression(TryExpression node, Scope scope)
        {
            return call(null, "try", lambda(
                    new LambdaTypeStamp(null, null), // TODO: lambda type
                    translate(node.getInnerExpression(), scope))); // TODO: type
        }

        @Override
        protected Expression visitLikePredicate(LikePredicate node, Scope scope)
        {
            if (node.getEscape() == null) {
                return call(
                        null, // TODO: type
                        "like",
                        translate(node.getValue(), scope),
                        translate(node.getPattern(), scope));
            }

            return call(
                    null,
                    "like", // TODO: type
                    translate(node.getValue(), scope),
                    translate(node.getPattern(), scope),
                    translate(node.getEscape(), scope));
        }

        @Override
        protected Expression visitInPredicate(InPredicate node, Scope scope)
        {
            return call(
                    null,
                    "in", // TODO: type
                    translate(node.getValue(), scope),
                    translate(node.getValueList(), scope));
        }

        @Override
        protected Expression visitInListExpression(InListExpression node, Scope scope)
        {
            return call(
                    null, // TODO: type
                    "array",
                    node.getValues().stream()
                            .map(e -> translate(e, scope))
                            .collect(Collectors.toList()));
        }

        @Override
        protected Expression visitCoalesceExpression(CoalesceExpression node, Scope scope)
        {
            return call(
                    null, // TODO: type
                    "coalesce",
                    node.getOperands().stream()
                            .map(e -> translate(e, scope))
                            .collect(toList()));
        }

        @Override
        protected Expression visitFunctionCall(FunctionCall node, Scope scope)
        {
            return call(
                    null, // TODO: type
                    node.getName().toString(),
                    node.getArguments().stream()
                            .map(argument -> translate(argument, scope))
                            .collect(toImmutableList()));
        }

        @Override
        protected Expression visitIfExpression(IfExpression node, Scope scope)
        {
            return call(
                    null, // TODO: type
                    "if",
                    process(node.getCondition(), scope),
                    process(node.getTrueValue(), scope),
                    node.getFalseValue().map(e -> process(e, scope)).orElse(new Null(null))); // TODO: type
        }

        @Override
        protected Expression visitNullIfExpression(NullIfExpression node, Scope scope)
        {
            return call(
                    null, // TODO: type
                    "nullif",
                    process(node.getFirst(), scope),
                    process(node.getSecond(), scope));
        }

        @Override
        protected Expression visitComparisonExpression(ComparisonExpression node, Scope scope)
        {
            // TODO: signature of operator
            return call(
                    null, // TODO: type
                    node.getType().toString(),
                    process(node.getLeft(), scope),
                    process(node.getRight(), scope));
        }

        @Override
        protected Expression visitArithmeticBinary(ArithmeticBinaryExpression node, Scope scope)
        {
            // TODO: signature of operator
            return call(
                    null, // TODO: type
                    node.getType().toString(),
                    process(node.getLeft(), scope),
                    process(node.getRight(), scope));
        }

        @Override
        protected Expression visitNotExpression(NotExpression node, Scope scope)
        {
            return call(null, "not", process(node.getValue(), scope)); // TODO: type
        }

        @Override
        protected Expression visitLogicalBinaryExpression(LogicalBinaryExpression node, Scope scope)
        {
            // TODO: signature of operator
            return call(
                    null, // TODO: type
                    node.getType().toString(),
                    process(node.getLeft(), scope),
                    process(node.getRight(), scope));
        }

        @Override
        protected Expression visitIsNotNullPredicate(IsNotNullPredicate node, Scope scope)
        {
            // TODO: signature of operator
            return call(
                    null, // TODO: type
                    "not",
                    call(
                            null, // TODO: type
                            "is-null",
                            translate(node.getValue(), scope)));
        }

        @Override
        protected Expression visitCast(Cast node, Scope scope)
        {
            return call(
                    null, // TODO: type
                    "cast",
                    translate(node.getExpression(), scope),
                    value(null, node.getType())); // TODO: type
        }

        @Override
        protected Expression visitIsNullPredicate(IsNullPredicate node, Scope scope)
        {
            // TODO: signature of operator
            return call(null, "is-null", translate(node.getValue(), scope)); // TODO: type
        }

        @Override
        protected Expression visitArithmeticUnary(ArithmeticUnaryExpression node, Scope scope)
        {
            // TODO: signature of operator
            return call(null, node.getSign().toString(), translate(node.getValue(), scope)); // TODO: type
        }

        @Override
        protected Expression visitBetweenPredicate(BetweenPredicate node, Scope scope)
        {
            // TODO: signature of operator
            return call(
                    null, // TODO: type
                    "between",
                    translate(node.getValue(), scope),
                    translate(node.getMin(), scope),
                    translate(node.getMax(), scope));
        }

        @Override
        protected Expression visitSymbolReference(SymbolReference node, Scope scope)
        {
            int currentLevel = 0;
            while (true) {
                final int level = currentLevel;
                Optional<Expression> resolved = scope.resolver.resolve(node.getName(), (type) -> reference(type, level));
                if (resolved.isPresent()) {
                    return resolved.get();
                }
                checkArgument(scope.parent.isPresent(), "Symbol '%s' not found in scope", node.getName());
                currentLevel++;
                scope = scope.parent.get();
            }
        }

        @Override
        protected Expression visitArrayConstructor(ArrayConstructor node, Scope scope)
        {
            return call(
                    null, // TODO: type
                    "array",
                    node.getValues().stream()
                            .map(value -> translate(value, scope)) // TODO: type
                            .collect(toList()));
        }

        @Override
        protected Expression visitSubscriptExpression(SubscriptExpression node, Scope scope)
        {
            return call(
                    null,
                    "subscript", // TODO: type
                    translate(node.getBase(), scope),
                    translate(node.getIndex(), scope));
        }

        @Override
        protected Expression visitExtract(Extract node, Scope scope)
        {
            return call(
                    null, // TODO: type
                    "extract",
                    translate(node.getExpression(), scope),
                    value(null, node.getField())); // TODO: type
        }

        @Override
        protected Expression visitSimpleCaseExpression(SimpleCaseExpression node, Scope scope)
        {
            List<Expression> conditions = node.getWhenClauses().stream()
                    .map(WhenClause::getOperand)
                    .map(e -> translate(e, scope))
                    .collect(toList());

            List<Expression> results = node.getWhenClauses().stream()
                    .map(WhenClause::getResult)
                    .map(e -> lambda(
                            new LambdaTypeStamp(null, null), // TODO: lambda type
                            translate(e, new Scope(scope, (name, localVariable) -> Optional.empty()))))
                    .collect(toList());

            Lambda defaultResult = node.getDefaultValue()
                    .map(e -> lambda(
                            new LambdaTypeStamp(null, null), // TODO: lambda type
                            translate(e, new Scope(scope, (name, localVariable) -> Optional.empty()))))
                    .orElse(lambda(
                            new LambdaTypeStamp(null, null), // TODO: lambda type
                            new Null(null))); // TODO: type

            return call(
                    null, // TODO: type
                    "lookup-switch",
                    translate(node.getOperand(), scope),
                    call(null, "array", conditions), // TODO: type
                    call(null, "array", results), // TODO: type
                    defaultResult);
        }

        @Override
        protected Expression visitSearchedCaseExpression(SearchedCaseExpression node, Scope scope)
        {
            List<Expression> conditions = node.getWhenClauses().stream()
                    .map(WhenClause::getOperand)
                    .map(e -> lambda(
                            new LambdaTypeStamp(null, null), // TODO: lambda type
                            translate(e, new Scope(scope, (name, localVariable) -> Optional.empty()))))
                    .collect(toList());

            List<Expression> results = node.getWhenClauses().stream()
                    .map(WhenClause::getResult)
                    .map(e -> lambda(
                            new LambdaTypeStamp(null, null), // TODO: lambda type
                            translate(e, new Scope(scope, (name, localVariable) -> Optional.empty()))))
                    .collect(toList());

            Lambda defaultResult = node.getDefaultValue()
                    .map(e -> lambda(
                            new LambdaTypeStamp(null, null), // TODO: lambda type
                            translate(e, new Scope(scope, (name, localVariable) -> Optional.empty()))))
                    .orElse(lambda(
                            new LambdaTypeStamp(null, null), // TODO: lambda type
                            new Null(null))); // TODO: type

            return call(
                    null, // TODO: type
                    "case",
                    call(null, "array", conditions), // TODO: type
                    call(null, "array", results),    // TODO: type
                    defaultResult);
        }
    }

    public static void main(String[] args)
    {
        ValuesNode values = new ValuesNode(
                new PlanNodeId("0"),
                list(new Symbol("a"), new Symbol("b")),
                list(
                        list(new LongLiteral("1"), new BooleanLiteral("true")),
                        list(new LongLiteral("2"), new BooleanLiteral("false")),
                        list(new LongLiteral("3"), new BooleanLiteral("true"))
                ));

        FilterNode filter1 = new FilterNode(
                new PlanNodeId("1"),
                values,
                new LogicalBinaryExpression(LogicalBinaryExpression.Type.OR,
                        new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND,
                                new ComparisonExpression(ComparisonExpressionType.EQUAL, new SymbolReference("a"), new SymbolReference("r")),
                                new IsNullPredicate(new SymbolReference("b"))),
                        new IsNotNullPredicate(new SymbolReference("a"))));

        FilterNode filter2 = new FilterNode(
                new PlanNodeId("2"),
                filter1,
                new ComparisonExpression(ComparisonExpressionType.EQUAL, new SymbolReference("a"), new LongLiteral("6")));

        Map<Symbol, com.facebook.presto.sql.tree.Expression> assignments = ImmutableMap.<Symbol, com.facebook.presto.sql.tree.Expression>builder()
                .put(new Symbol("x"), new ArithmeticUnaryExpression(ArithmeticUnaryExpression.Sign.MINUS, new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.ADD, new SymbolReference("a"), new LongLiteral("10"))))
                .put(new Symbol("y"), new Cast(new SymbolReference("b"), "varchar(10)"))
                .put(new Symbol("z"), new BetweenPredicate(new SymbolReference("a"), new LongLiteral("1"), new LongLiteral("5")))
                .put(new Symbol("w"), new FunctionCall(QualifiedName.of("foo"), list(new SymbolReference("a"))))
                .build();

        ProjectNode project = new ProjectNode(new PlanNodeId("3"), filter2, assignments);

        Expression translated = new LegacyToNew(ImmutableMap.of())
                .translate(project, new Scope(forNames(list("r"), list(BigintType.BIGINT))));

        throw new UnsupportedOperationException("not yet implemented");
//        GreedyOptimizer optimizer = new GreedyOptimizer(true);
//        Expression optimized = optimizer.optimize(translated);
//        System.out.println(translated);
//        System.out.println();
//        System.out.println(optimized);
    }
}
