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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.newplanner.expression.AggregationExpression;
import com.facebook.presto.sql.newplanner.expression.FilterExpression;
import com.facebook.presto.sql.newplanner.expression.GroupByAggregationExpression;
import com.facebook.presto.sql.newplanner.expression.InlineTableExpression;
import com.facebook.presto.sql.newplanner.expression.LimitExpression;
import com.facebook.presto.sql.newplanner.expression.MarkDistinctExpression;
import com.facebook.presto.sql.newplanner.expression.ProjectExpression;
import com.facebook.presto.sql.newplanner.expression.RelationalExpression;
import com.facebook.presto.sql.newplanner.expression.SortExpression;
import com.facebook.presto.sql.newplanner.expression.TableExpression;
import com.facebook.presto.sql.newplanner.expression.TopNExpression;
import com.facebook.presto.sql.newplanner.expression.UnionExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SinkNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableCommitNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.Expressions;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.analyzeExpressionsWithSymbols;
import static com.google.common.base.Functions.forMap;
import static com.google.common.base.Preconditions.checkArgument;

public class PlanToRelationalExpressionTranslator
{
    public RelationalExpression translate(PlanNode root, Metadata metadata, SqlParser parser, Session session, Map<Symbol, Type> types)
    {
        TranslationResult result = root.accept(new Visitor(metadata, parser, session, types), null);
        return result.getExpression();
    }

    private static class Visitor
            extends PlanVisitor<Void, TranslationResult>
    {
        private final Metadata metadata;
        private final SqlParser parser;
        private final Session session;
        private final Map<Symbol, Type> types;

        private int nextId = 0;

        public Visitor(Metadata metadata, SqlParser parser, Session session, Map<Symbol, Type> types)
        {
            this.metadata = metadata;
            this.parser = parser;
            this.session = session;
            this.types = types;
        }

        private int nextId()
        {
            return nextId++;
        }

        @Override
        public TranslationResult visitAggregation(AggregationNode node, Void context)
        {
            TranslationResult child = node.getSource().accept(this, null);

            if (node.getGroupBy().isEmpty()) {
                List<Type> outputTypes = new ArrayList<>();
                List<Signature> aggregates = new ArrayList<>();
                List<List<Integer>> arguments = new ArrayList<>();
                List<Optional<Integer>> filters = new ArrayList<>();
                for (Symbol output : node.getOutputSymbols()) {
                    outputTypes.add(types.get(output));

                    Symbol mask = node.getMasks().get(output);
                    if (mask != null) {
                        filters.add(Optional.of(child.getSymbolToFieldMapping().get(mask)));
                    }
                    else {
                        filters.add(Optional.<Integer>absent());
                    }

                    FunctionCall aggregation = node.getAggregations().get(output);
                    aggregates.add(node.getFunctions().get(output));

                    List<Integer> args = new ArrayList<>();
                    for (Expression expression : aggregation.getArguments()) {
                        Symbol symbol = Symbol.fromQualifiedName(((QualifiedNameReference) expression).getName());
                        args.add((child.getSymbolToFieldMapping().get(symbol)));
                    }
                    arguments.add(args);
                }
                AggregationExpression result = new AggregationExpression(nextId(), child.getExpression(), new RelationalExpressionType(outputTypes), aggregates, filters, arguments);
                return new TranslationResult(result, node.getOutputSymbols());
            }
            else {
                List<Type> outputTypes = new ArrayList<>();
                List<Signature> aggregates = new ArrayList<>();
                List<List<Integer>> arguments = new ArrayList<>();
                List<Optional<Integer>> filters = new ArrayList<>();
                List<Integer> groupingInputs = new ArrayList<>();

                for (Symbol symbol : node.getGroupBy()) {
                    groupingInputs.add(child.getSymbolToFieldMapping().get(symbol));
                    outputTypes.add(types.get(symbol));
                }

                for (Symbol aggregate : node.getAggregations().keySet()) {
                    outputTypes.add(types.get(aggregate));

                    Symbol mask = node.getMasks().get(aggregate);
                    if (mask != null) {
                        filters.add(Optional.of(child.getSymbolToFieldMapping().get(mask)));
                    }
                    else {
                        filters.add(Optional.<Integer>absent());
                    }

                    FunctionCall aggregation = node.getAggregations().get(aggregate);
                    aggregates.add(node.getFunctions().get(aggregate));

                    List<Integer> args = new ArrayList<>();
                    for (Expression expression : aggregation.getArguments()) {
                        Symbol symbol = Symbol.fromQualifiedName(((QualifiedNameReference) expression).getName());
                        args.add((child.getSymbolToFieldMapping().get(symbol)));
                    }
                    arguments.add(args);
                }


                GroupByAggregationExpression result = new GroupByAggregationExpression(nextId(), child.getExpression(), new RelationalExpressionType(outputTypes), groupingInputs, aggregates, filters, arguments);
                return new TranslationResult(result, node.getOutputSymbols());
            }
        }

        @Override
        public TranslationResult visitFilter(FilterNode node, Void context)
        {
            IdentityHashMap<Expression, Type> expressionTypes = analyzeExpressions(ImmutableList.of(node.getPredicate()), node.getSource().getOutputSymbols());

            TranslationResult child = node.getSource().accept(this, null);
            RowExpression condition = SqlToRowExpressionTranslator.translate(node.getPredicate(), expressionTypes, child.getSymbolToFieldMapping(), metadata, session, false);

            FilterExpression result = new FilterExpression(nextId(), child.getExpression(), condition);
            return new TranslationResult(result, node.getOutputSymbols());
        }

        @Override
        public TranslationResult visitProject(ProjectNode node, Void context)
        {
            IdentityHashMap<Expression, Type> expressionTypes = analyzeExpressions(node.getExpressions(), node.getSource().getOutputSymbols());

            TranslationResult child = node.getSource().accept(this, null);
            ImmutableList.Builder<RowExpression> projections = ImmutableList.builder();
            for (Symbol output : node.getOutputSymbols()) {
                Expression expression = node.getAssignments().get(output);
                projections.add(SqlToRowExpressionTranslator.translate(expression, expressionTypes, child.getSymbolToFieldMapping(), metadata, session, false));
            }

            ProjectExpression result = new ProjectExpression(nextId(), child.getExpression(), projections.build());
            return new TranslationResult(result, node.getOutputSymbols());
        }

        @Override
        public TranslationResult visitTopN(TopNNode node, Void context)
        {
            TranslationResult child = node.getSource().accept(this, null);

            List<Integer> sortFields = IterableTransformer.on(node.getOrderBy())
                    .transform(forMap(child.getSymbolToFieldMapping()))
                    .list();

            List<SortOrder> sortOrders = IterableTransformer.on(node.getOrderBy())
                    .transform(forMap(node.getOrderings()))
                    .list();

            TopNExpression result = new TopNExpression(nextId(), child.getExpression(), sortFields, sortOrders, node.getCount());
            return new TranslationResult(result, node.getOutputSymbols());
        }

        @Override
        public TranslationResult visitOutput(OutputNode node, Void context)
        {
            TranslationResult child = node.getSource().accept(this, null);

            List<Symbol> sourceSymbols = child.getOutputs();
            List<Symbol> resultSymbols = node.getOutputSymbols();
            if (resultSymbols.equals(sourceSymbols)) {
                // no projection needed
                return child;
            }

            ImmutableList.Builder<RowExpression> projections = ImmutableList.builder();
            for (Symbol output : node.getOutputSymbols()) {
                projections.add(Expressions.field(sourceSymbols.indexOf(output), types.get(output)));
            }

            ProjectExpression result = new ProjectExpression(nextId(), child.getExpression(), projections.build());
            return new TranslationResult(result, node.getOutputSymbols());
        }

        @Override
        public TranslationResult visitLimit(LimitNode node, Void context)
        {
            TranslationResult child = node.getSource().accept(this, null);

            LimitExpression result = new LimitExpression(nextId(), child.getExpression(), node.getCount());
            return new TranslationResult(result, node.getOutputSymbols());
        }

        @Override
        public TranslationResult visitDistinctLimit(DistinctLimitNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        public TranslationResult visitSample(SampleNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        public TranslationResult visitTableScan(TableScanNode node, Void context)
        {
            ImmutableList.Builder<ColumnHandle> columns = ImmutableList.builder();
            ImmutableList.Builder<Type> rowType = ImmutableList.builder();

            for (Symbol output : node.getOutputSymbols()) {
                columns.add(node.getAssignments().get(output));
                rowType.add(types.get(output));
            }

            TableExpression result = new TableExpression(nextId(), node.getTable(), columns.build(), new RelationalExpressionType(rowType.build()));
            return new TranslationResult(result, node.getOutputSymbols());
        }

        @Override
        public TranslationResult visitValues(ValuesNode node, Void context)
        {
            ImmutableList.Builder<Type> rowType = ImmutableList.builder();

            for (Symbol output : node.getOutputSymbols()) {
                rowType.add(types.get(output));
            }

            ImmutableList.Builder<List<ConstantExpression>> rowsBuilder = ImmutableList.builder();
            for (List<Expression> row : node.getRows()) {
                ImmutableList.Builder<ConstantExpression> rowBuilder = ImmutableList.builder();
                for (Expression expression : row) {
                    IdentityHashMap<Expression, Type> expressionTypes = analyzeExpressions(ImmutableList.of(expression), ImmutableList.<Symbol>of());
                    RowExpression translated = SqlToRowExpressionTranslator.translate(expression, expressionTypes, ImmutableMap.<Symbol, Integer>of(), metadata, session, true);

                    checkArgument(translated instanceof ConstantExpression, "Expression in inline table must be constant");
                    rowBuilder.add((ConstantExpression) translated);
                }

                rowsBuilder.add(rowBuilder.build());
            }

            InlineTableExpression result = new InlineTableExpression(nextId(), rowType.build(), rowsBuilder.build());
            return new TranslationResult(result, node.getOutputSymbols());
        }

        @Override
        public TranslationResult visitIndexSource(IndexSourceNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        public TranslationResult visitJoin(JoinNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        public TranslationResult visitSemiJoin(SemiJoinNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        public TranslationResult visitIndexJoin(IndexJoinNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        public TranslationResult visitSort(SortNode node, Void context)
        {
            TranslationResult child = node.getSource().accept(this, null);

            List<Integer> sortFields = IterableTransformer.on(node.getOrderBy())
                    .transform(forMap(child.getSymbolToFieldMapping()))
                    .list();

            List<SortOrder> sortOrders = IterableTransformer.on(node.getOrderBy())
                    .transform(forMap(node.getOrderings()))
                    .list();

            SortExpression result = new SortExpression(nextId(), child.getExpression(), sortFields, sortOrders);
            return new TranslationResult(result, node.getOutputSymbols());
        }

        @Override
        public TranslationResult visitSink(SinkNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        public TranslationResult visitWindow(WindowNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        public TranslationResult visitTableWriter(TableWriterNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        public TranslationResult visitTableCommit(TableCommitNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        public TranslationResult visitUnion(UnionNode node, Void context)
        {
            List<Type> types = IterableTransformer.on(node.getOutputSymbols())
                    .transform(forMap(this.types))
                    .list();

            ImmutableList.Builder<RelationalExpression> children = ImmutableList.builder();
            for (PlanNode child : node.getSources()) {
                children.add(child.accept(this, null).getExpression());
            }

            UnionExpression result = new UnionExpression(nextId(), new RelationalExpressionType(types), children.build());
            return new TranslationResult(result, node.getOutputSymbols());
        }

        @Override
        public TranslationResult visitMarkDistinct(MarkDistinctNode node, Void context)
        {
            TranslationResult child = node.getSource().accept(this, null);

            List<Integer> distinctFields = IterableTransformer.on(node.getDistinctSymbols())
                    .transform(forMap(child.getSymbolToFieldMapping()))
                    .list();

            MarkDistinctExpression result = new MarkDistinctExpression(nextId(), child.getExpression(), distinctFields);
            return new TranslationResult(result, node.getOutputSymbols());
        }

        @Override
        public TranslationResult visitTopNRowNumber(TopNRowNumberNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        private IdentityHashMap<Expression, Type> analyzeExpressions(List<Expression> expressions, List<Symbol> parentOutputs)
        {
            Map<Symbol, Type> symbolTypes = IterableTransformer.on(parentOutputs).toMap(forMap(types)).map();
            return analyzeExpressionsWithSymbols(session, metadata, parser, symbolTypes, expressions).getExpressionTypes();
        }
    }

    private static class TranslationResult
    {
        private final List<Symbol> outputs;
        private final RelationalExpression expression;

        private TranslationResult(RelationalExpression expression, List<Symbol> outputs)
        {
            this.expression = expression;
            this.outputs = outputs;
        }

        public List<Symbol> getOutputs()
        {
            return outputs;
        }

        public Map<Symbol, Integer> getSymbolToFieldMapping()
        {
            // TODO: do this in constructor
            ImmutableMap.Builder<Symbol, Integer> builder = ImmutableMap.builder();
            for (int i = 0; i < outputs.size(); i++) {
                Symbol symbol = outputs.get(i);
                builder.put(symbol, i);
            }

            return builder.build();
        }

        public RelationalExpression getExpression()
        {
            return expression;
        }
    }
}
