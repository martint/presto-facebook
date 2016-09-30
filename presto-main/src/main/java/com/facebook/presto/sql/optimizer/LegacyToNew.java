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
import com.facebook.presto.sql.optimizer.tree.Value;
import com.facebook.presto.sql.optimizer.tree.sql.Null;
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
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BinaryLiteral;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
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
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.TryExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import static java.util.stream.Collectors.toList;

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
        Optional<Expression> resolve(String name, Expression localVariable);
    }

    static Resolver forSymbols(List<Symbol> symbols)
    {
        return forNames(symbols.stream()
                .map(Symbol::getName)
                .collect(toImmutableList()));
    }

    static Resolver forNames(List<String> names)
    {
        return (name, localVariable) -> {
            if (names.contains(name)) {
                return Optional.of(fieldDereference(localVariable, names.indexOf(name)));
            }

            return Optional.empty();
        };
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
        public Expression visitEnforceSingleRow(EnforceSingleRowNode node, Scope scope)
        {
            return call("enforce-single-row", translate(node.getSource(), scope));
        }

        @Override
        public Expression visitLimit(LimitNode node, Scope scope)
        {
            return call("limit", translate(node.getSource(), scope), value(node.getCount()));
        }

        @Override
        public Expression visitOutput(OutputNode node, Scope scope)
        {
            return call("transform",
                    translate(node.getSource(), scope),
                    lambda(call("row", node.getOutputSymbols().stream()
                            .map(output -> translate(output.toSymbolReference(), new Scope(scope, forSymbols(node.getSource().getOutputSymbols()))))
                            .collect(toList()))));
        }

        @Override
        public Expression visitFilter(FilterNode node, Scope scope)
        {
            Expression lambdaBody = translate(node.getPredicate(), new Scope(scope, forSymbols(node.getSource().getOutputSymbols())));

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
                                return translate(node.getAssignments().get(output), new Scope(scope, forSymbols(outputSymbols)));
                            })
                            .collect(toList()))));
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

        @Override
        public Expression visitSort(SortNode node, Scope scope)
        {
            List<Expression> criteria = node.getOrderBy().stream()
                    .map(input ->
                            call("row",
                                    lambda(translate(input.toSymbolReference(), new Scope(scope, forSymbols(node.getSource().getOutputSymbols())))),
                                    value(node.getOrderings().get(input))))
                    .collect(toImmutableList());

            return call("sort",
                    translate(node.getSource(), scope),
                    call("array", criteria));
        }

        @Override
        public Expression visitTopN(TopNNode node, Scope scope)
        {
            List<Expression> criteria = node.getOrderBy().stream()
                    .map(input ->
                            call("row",
                                    lambda(translate(input.toSymbolReference(), new Scope(scope, forSymbols(node.getSource().getOutputSymbols())))),
                                    value(node.getOrderings().get(input))))
                    .collect(toImmutableList());

            return call("top-n",
                    translate(node.getSource(), scope),
                    value(node.getCount()),
                    call("array", criteria));
        }

        @Override
        public Expression visitSample(SampleNode node, Scope scope)
        {
            checkArgument(!node.getSampleWeightSymbol().isPresent(), "weighted sample not supported");

            return call("sample",
                    translate(node.getSource(), scope),
                    value(node.getSampleType()),
                    value(node.getSampleRatio()));
        }

        @Override
        public Expression visitTableScan(TableScanNode node, Scope context)
        {
            // TODO: column ordering
            return call("table", value(node.getTable()));
        }

        @Override
        public Expression visitAggregation(AggregationNode node, Scope scope)
        {
            checkArgument(node.getMasks().isEmpty(), "distinct aggregations not supported");
            checkArgument(!node.getHashSymbol().isPresent(), "pre-computed hash not supported");

            Expression source = translate(node.getSource(), scope);

            List<Expression> calls = node.getOutputSymbols().stream()
                    .filter(node.getAggregations()::containsKey)
                    .map(output -> lambda(translate(node.getAggregations().get(output), new Scope(scope, forSymbols(node.getSource().getOutputSymbols())))))
                    .collect(toList());

            if (node.getGroupIdSymbol().isPresent()) {
                List<List<Value>> groupingSets = node.getGroupingSets().stream()
                        .map(set ->
                                set.stream()
                                        .map(column -> value(node.getSource().getOutputSymbols().indexOf(column)))
                                        .collect(toList()))
                        .collect(toList());

                // TODO functions, mask, etc
                return call("grouping-sets",
                        source,
                        call("array",
                                value(groupingSets)),
                        value(node.getSource().getOutputSymbols().indexOf(node.getGroupIdSymbol().get())));
            }
            else {
                return call("aggregation", source, call("array", calls)); // TODO functions, mask, etc
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
            List<String> rightFields = names(node.getRight().getOutputSymbols());

            Scope scope = new Scope(parentScope, (name, localVariable) -> {
                int index = leftFields.indexOf(name);
                if (index != -1) {
                    return Optional.of(fieldDereference(fieldDereference(localVariable, 0), index));
                }

                index = rightFields.indexOf(name);
                if (index != -1) {
                    return Optional.of(fieldDereference(fieldDereference(localVariable, 1), index));
                }

                return Optional.empty();
            });

            Expression criteria = null;
            for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
                Expression term = call(ComparisonExpression.Type.EQUAL.toString(), translate(clause.getLeft().toSymbolReference(), scope), translate(clause.getRight().toSymbolReference(), scope));
                if (criteria == null) {
                    criteria = term;
                }
                else {
                    criteria = call("and", criteria, term);
                }
            }

            if (node.getFilter().isPresent()) {
                Expression term = translate(node.getFilter().get(), scope);
                if (criteria == null) {
                    criteria = term;
                }
                else {
                    criteria = call("and", criteria, term);
                }
            }

            if (criteria == null) {
                criteria = lambda(value(true));
            }

            return call("join",
                    left,
                    right,
                    lambda(criteria),
                    value(node.getType()));
        }

        @Override
        public Expression visitApply(ApplyNode node, Scope scope)
        {
            return call(
                    "apply",
                    translate(node.getInput(), scope),
                    translate(node.getSubquery(), new Scope(scope, forSymbols(node.getInput().getOutputSymbols()))));
        }

        @Override
        public Expression visitUnion(UnionNode node, Scope scope)
        {
            return call("concat", translateSetOperationChildren(node, scope));
        }

        @Override
        public Expression visitIntersect(IntersectNode node, Scope scope)
        {
            return call("intersect", translateSetOperationChildren(node, scope));
        }

        @Override
        public Expression visitExcept(ExceptNode node, Scope scope)
        {
            List<Expression> children = translateSetOperationChildren(node, scope);

            Expression left = children.get(0);
            Expression right;
            if (children.size() > 2) {
                right = call("concat", children.subList(1, children.size()));
            }
            else {
                right = children.get(1);
            }

            return call("except", left, right);
        }

        @Override
        public Expression visitWindow(WindowNode node, Scope scope)
        {
            checkArgument(!node.getHashSymbol().isPresent(), "pre-computed hash not supported");

            Expression source = translate(node.getSource(), scope);

            List<Expression> orderings = node.getOrderBy().stream()
                    .map(input ->
                            call("row",
                                    lambda(translate(input.toSymbolReference(), new Scope(scope, forSymbols(node.getSource().getOutputSymbols())))),
                                    value(node.getOrderings().get(input))))
                    .collect(toImmutableList());

            List<Expression> partitionColumns = node.getPartitionBy().stream()
                    .map(column -> value(node.getSource().getOutputSymbols().indexOf(column)))
                    .collect(toList());

            // TODO: functions & frames
            return call("window",
                    source,
                    call("array", partitionColumns),
                    call("array", orderings));
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
                    .map(e -> lambda(fieldDereference(localReference(), node.getSource().getOutputSymbols().indexOf(e))))
                    .collect(toList());

            return call("unnest",
                    translate(node.getSource(), scope),
                    call("array", columns));
        }

        @Override
        public Expression visitGroupId(GroupIdNode node, Scope scope)
        {
            List<List<Value>> groupingSets = node.getGroupingSets().stream()
                    .map(set ->
                            set.stream()
                                    .map(column -> value(node.getSource().getOutputSymbols().indexOf(column)))
                                    .collect(toList()))
                    .collect(toList());

            return call("group-id",
                    translate(node.getSource(), scope),
                    value(groupingSets));
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
                        call("transform",
                                translate(source, scope),
                                lambda(call("row", node.getOutputSymbols().stream()
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

    private static class ExpressionTranslator
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
            return call("row",
                    node.getItems().stream()
                            .map(item -> translate(item, scope))
                            .collect(toList()));
        }

        @Override
        protected Expression visitDoubleLiteral(DoubleLiteral node, Scope context)
        {
            return value(node.getValue());
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
        protected Expression visitDereferenceExpression(DereferenceExpression node, Scope scope)
        {
            return fieldDereference(translate(node.getBase(), scope), node.getFieldName());
        }

        @Override
        protected Expression visitLambdaExpression(LambdaExpression node, Scope scope)
        {
            return lambda(translate(node.getBody(), new Scope(scope, forNames(node.getArguments()))));
        }

        @Override
        protected Expression visitTryExpression(TryExpression node, Scope scope)
        {
            return call("try", lambda(translate(node.getInnerExpression(), scope)));
        }

        @Override
        protected Expression visitLikePredicate(LikePredicate node, Scope scope)
        {
            if (node.getEscape() == null) {
                return call("like",
                        translate(node.getValue(), scope),
                        translate(node.getPattern(), scope));
            }

            return call("like",
                    translate(node.getValue(), scope),
                    translate(node.getPattern(), scope),
                    translate(node.getEscape(), scope));
        }

        @Override
        protected Expression visitInPredicate(InPredicate node, Scope scope)
        {
            return call("in",
                    translate(node.getValue(), scope),
                    translate(node.getValueList(), scope));
        }

        @Override
        protected Expression visitInListExpression(InListExpression node, Scope scope)
        {
            return call("array",
                    node.getValues().stream()
                            .map(e -> translate(e, scope))
                            .collect(Collectors.toList()));
        }

        @Override
        protected Expression visitCoalesceExpression(CoalesceExpression node, Scope scope)
        {
            return call("coalesce",
                    node.getOperands().stream()
                            .map(e -> translate(e, scope))
                            .collect(toList()));
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
        protected Expression visitIfExpression(IfExpression node, Scope scope)
        {
            return call("if",
                    process(node.getCondition(), scope),
                    process(node.getTrueValue(), scope),
                    node.getFalseValue().map(e -> process(e, scope)).orElse(new Null()));
        }

        @Override
        protected Expression visitNullIfExpression(NullIfExpression node, Scope scope)
        {
            return call("nullif",
                    process(node.getFirst(), scope),
                    process(node.getSecond(), scope));
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
        protected Expression visitNotExpression(NotExpression node, Scope scope)
        {
            return call("not", process(node.getValue(), scope));
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
                Optional<Expression> resolved = scope.resolver.resolve(node.getName(), reference(level));
                if (resolved.isPresent()) {
                    return resolved.get();
                }
                checkArgument(scope.parent.isPresent(), "Symbol '%s' not found in scope", node.getName());
                level++;
                scope = scope.parent.get();
            }
        }

        @Override
        protected Expression visitArrayConstructor(ArrayConstructor node, Scope scope)
        {
            return call("array",
                    node.getValues().stream()
                            .map(value -> translate(value, scope))
                            .collect(toList()));
        }

        @Override
        protected Expression visitSubscriptExpression(SubscriptExpression node, Scope scope)
        {
            return call("subscript",
                    translate(node.getBase(), scope),
                    translate(node.getIndex(), scope));
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

        Expression translated = translate(project, new Scope(forNames(list("r"))));

        GreedyOptimizer optimizer = new GreedyOptimizer(true);
        Expression optimized = optimizer.optimize(translated);

        System.out.println(translated);
        System.out.println();
        System.out.println(optimized);
    }
}
