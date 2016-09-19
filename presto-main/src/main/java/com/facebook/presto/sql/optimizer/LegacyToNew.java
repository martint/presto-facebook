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

import com.facebook.presto.sql.optimizer.engine.GreedyOptimizer;
import com.facebook.presto.sql.optimizer.tree.Expression;
import com.facebook.presto.sql.optimizer.tree.sql.Null;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BinaryLiteral;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CharLiteral;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.FieldReference;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.optimizer.tree.Expressions.call;
import static com.facebook.presto.sql.optimizer.tree.Expressions.fieldDereference;
import static com.facebook.presto.sql.optimizer.tree.Expressions.lambda;
import static com.facebook.presto.sql.optimizer.tree.Expressions.reference;
import static com.facebook.presto.sql.optimizer.tree.Expressions.value;
import static com.facebook.presto.sql.optimizer.utils.CollectionConstructors.list;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;

public class LegacyToNew
{
    private LegacyToNew()
    {
    }

    public static Expression translate(PlanNode node)
    {
        return translate(node, new Scope());
    }

    private static Expression translate(PlanNode node, Scope scope)
    {
        return node.accept(new PlanTranslator(), scope);
    }

    private static Expression translate(com.facebook.presto.sql.tree.Expression expression, Scope scope)
    {
        return new ExpressionTranslator().process(expression, scope);
    }

    private static class Scope
    {
        private final Optional<Scope> parent;
        private final List<String> bindings;

        public Scope()
        {
            this.parent = Optional.empty();
            this.bindings = list();
        }

        public Scope(List<String> bindings)
        {
            this(Optional.empty(), bindings);
        }

        public Scope(Scope parent, List<String> bindings)
        {
            this(Optional.of(parent), bindings);
        }

        private Scope(Optional<Scope> parent, List<String> bindings)
        {
            this.parent = parent;
            this.bindings = ImmutableList.copyOf(bindings);
        }
    }

    private static class PlanTranslator
            extends PlanVisitor<Scope, Expression>
    {
        @Override
        protected Expression visitPlan(PlanNode node, Scope scope)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        @Override
        public Expression visitOutput(OutputNode node, Scope scope)
        {
            return call("transform",
                    translate(node.getSource(), scope),
                    lambda(call("row", node.getOutputSymbols().stream()
                            .map(output -> translate(output.toSymbolReference(), new Scope(scope, names(node.getSource().getOutputSymbols()))))
                            .collect(Collectors.toList()))));
        }

        @Override
        public Expression visitFilter(FilterNode node, Scope scope)
        {
            Expression lambdaBody = translate(node.getPredicate(), new Scope(scope, names(node.getSource().getOutputSymbols())));

            return call("logical-filter", translate(node.getSource(), scope), lambda(lambdaBody));
        }

        @Override
        public Expression visitProject(ProjectNode node, Scope scope)
        {
            return call("transform",
                    translate(node.getSource(), scope),
                    lambda(call("row", node.getOutputSymbols().stream()
                            .map(output -> {
                                List<Symbol> outputSymbols = node.getSource().getOutputSymbols();
                                return translate(node.getAssignments().get(output), new Scope(scope, names(outputSymbols)));
                            })
                            .collect(Collectors.toList()))));
        }

        @Override
        public Expression visitValues(ValuesNode node, Scope scope)
        {
            return call("array", node.getRows().stream()
                    .map(row -> call("row", row.stream()
                            .map(column -> translate(column, scope))
                            .collect(toImmutableList())))
                    .collect(toImmutableList()));
        }
    }

    private static class ExpressionTranslator
            extends AstVisitor<Expression, Scope>
    {
        @Override
        protected Expression visitNode(Node node, Scope context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        @Override
        protected Expression visitBooleanLiteral(BooleanLiteral node, Scope context)
        {
            return value(node.getValue());
        }

        @Override
        protected Expression visitStringLiteral(StringLiteral node, Scope context)
        {
            return value(node.getSlice());
        }

        @Override
        protected Expression visitLongLiteral(LongLiteral node, Scope context)
        {
            return value(node.getValue());
        }

        @Override
        protected Expression visitBinaryLiteral(BinaryLiteral node, Scope context)
        {
            return value(node.getValue());
        }

        @Override
        protected Expression visitNullLiteral(NullLiteral node, Scope context)
        {
            return new Null();
        }

        @Override
        protected Expression visitGenericLiteral(GenericLiteral node, Scope context)
        {
            return call(node.getType(), value(node.getValue()));
        }

        @Override
        protected Expression visitCoalesceExpression(CoalesceExpression node, Scope context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        protected Expression visitCharLiteral(CharLiteral node, Scope context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        protected Expression visitDecimalLiteral(DecimalLiteral node, Scope context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        protected Expression visitDoubleLiteral(DoubleLiteral node, Scope context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        protected Expression visitDereferenceExpression(DereferenceExpression node, Scope scope)
        {
            return fieldDereference(translate(node.getBase(), scope), node.getFieldName());
        }

        @Override
        protected Expression visitLambdaExpression(LambdaExpression node, Scope scope)
        {
            return lambda(translate(node.getBody(), new Scope(scope, node.getArguments())));
        }

        @Override
        protected Expression visitFieldReference(FieldReference node, Scope context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        protected Expression visitFunctionCall(FunctionCall node, Scope scope)
        {
            return call(
                    node.getName().toString(),
                    node.getArguments().stream()
                            .map(argument -> translate(argument, scope))
                            .collect(toImmutableList()));
        }

        @Override
        protected Expression visitComparisonExpression(ComparisonExpression node, Scope scope)
        {
            // TODO: signature of operator
            return call(
                    node.getType().toString(),
                    process(node.getLeft(), scope),
                    process(node.getRight(), scope));
        }

        @Override
        protected Expression visitArithmeticBinary(ArithmeticBinaryExpression node, Scope scope)
        {
            // TODO: signature of operator
            return call(
                    node.getType().toString(),
                    process(node.getLeft(), scope),
                    process(node.getRight(), scope));
        }

        @Override
        protected Expression visitLogicalBinaryExpression(LogicalBinaryExpression node, Scope scope)
        {
            // TODO: signature of operator
            return call(
                    node.getType().toString(),
                    process(node.getLeft(), scope),
                    process(node.getRight(), scope));
        }

        @Override
        protected Expression visitIsNotNullPredicate(IsNotNullPredicate node, Scope scope)
        {
            // TODO: signature of operator
            return call("not", call("is-null", translate(node.getValue(), scope)));
        }

        @Override
        protected Expression visitCast(Cast node, Scope scope)
        {
            return call("cast", translate(node.getExpression(), scope), value(node.getType()));
        }

        @Override
        protected Expression visitIsNullPredicate(IsNullPredicate node, Scope scope)
        {
            // TODO: signature of operator
            return call("is-null", translate(node.getValue(), scope));
        }

        @Override
        protected Expression visitArithmeticUnary(ArithmeticUnaryExpression node, Scope scope)
        {
            // TODO: signature of operator
            return call(node.getSign().toString(), translate(node.getValue(), scope));
        }

        @Override
        protected Expression visitBetweenPredicate(BetweenPredicate node, Scope scope)
        {
            // TODO: signature of operator
            return call("between",
                    translate(node.getValue(), scope),
                    translate(node.getMin(), scope),
                    translate(node.getMax(), scope));
        }

        @Override
        protected Expression visitSymbolReference(SymbolReference node, Scope scope)
        {
            int level = 0;
            while (true) {
                if (scope.bindings.contains(node.getName())) {
                    return fieldDereference(reference(level), scope.bindings.indexOf(node.getName()));
                }
                checkArgument(scope.parent.isPresent(), "Symbol '%s' not found in scope", node.getName());
                level++;
                scope = scope.parent.get();
            }
        }
    }

    private static List<String> names(List<Symbol> symbols)
    {
        return symbols.stream()
                .map(Symbol::getName)
                .collect(toImmutableList());
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
                                new ComparisonExpression(ComparisonExpression.Type.EQUAL, new SymbolReference("a"), new SymbolReference("r")),
                                new IsNullPredicate(new SymbolReference("b"))),
                        new IsNotNullPredicate(new SymbolReference("a"))));

        FilterNode filter2 = new FilterNode(
                new PlanNodeId("2"),
                filter1,
                new ComparisonExpression(ComparisonExpression.Type.EQUAL, new SymbolReference("a"), new LongLiteral("6")));

        Map<Symbol, com.facebook.presto.sql.tree.Expression> assignments = ImmutableMap.<Symbol, com.facebook.presto.sql.tree.Expression>builder()
                .put(new Symbol("x"), new ArithmeticUnaryExpression(ArithmeticUnaryExpression.Sign.MINUS, new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.ADD, new SymbolReference("a"), new LongLiteral("10"))))
                .put(new Symbol("y"), new Cast(new SymbolReference("b"), "varchar(10)"))
                .put(new Symbol("z"), new BetweenPredicate(new SymbolReference("a"), new LongLiteral("1"), new LongLiteral("5")))
                .put(new Symbol("w"), new FunctionCall(QualifiedName.of("foo"), list(new SymbolReference("a"))))
                .build();

        ProjectNode project = new ProjectNode(new PlanNodeId("3"), filter2, assignments);

        Expression translated = translate(project, new Scope(list("r")));

        GreedyOptimizer optimizer = new GreedyOptimizer(true);
        Expression optimized = optimizer.optimize(translated);

        System.out.println(translated);
        System.out.println();
        System.out.println(optimized);
    }
}
