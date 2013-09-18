package com.facebook.presto.sql.newplanner;

import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.FieldOrExpression;
import com.facebook.presto.sql.analyzer.TupleDescriptor;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.tree.*;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.sql.tree.FunctionCall.argumentsGetter;

public class NewPlanner
{
    private Analysis analysis;

    public RelationalExpression plan(Analysis analysis)
    {
        this.analysis = analysis;

        return new Visitor().process(analysis.getQuery(), new PlanningContext());
    }


    class Visitor
        extends DefaultTraversalVisitor<RelationalExpression, PlanningContext>
    {
        @Override
        protected RelationalExpression visitNode(Node node, PlanningContext context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        @Override
        protected RelationalExpression visitQuery(Query query, PlanningContext context)
        {
            RelationalExpression expression = process(query.getQueryBody(), context);

            //            List<FieldOrExpression> orderBy = analysis.getOrderByExpressions(query);
            //            List<FieldOrExpression> outputs = analysis.getOutputExpressions(query);
            //            builder = project(builder, Iterables.concat(orderBy, outputs));

            //            builder = sort(builder, query);
            //            builder = project(builder, analysis.getOutputExpressions(query));
            //            builder = limit(builder, query);

            return expression;
        }

//        private PlanBuilder aggregate(PlanBuilder subPlan, QuerySpecification node)
//        {
//            if (analysis.getAggregates(node).isEmpty() && analysis.getGroupByExpressions(node).isEmpty()) {
//                return subPlan;
//            }
//
//            Set<FieldOrExpression> arguments = IterableTransformer.on(analysis.getAggregates(node))
//                    .transformAndFlatten(argumentsGetter())
//                    .transform(toFieldOrExpression())
//                    .set();
//
//            // 1. Pre-project all scalar inputs (arguments and non-trivial group by expressions)
//            Iterable<FieldOrExpression> inputs = Iterables.concat(analysis.getGroupByExpressions(node), arguments);
//            if (!Iterables.isEmpty(inputs)) { // avoid an empty projection if the only aggregation is COUNT (which has no arguments)
//                subPlan = project(subPlan, inputs);
//            }
//
//            // 2. Aggregate
//            ImmutableMap.Builder<Symbol, FunctionCall> aggregationAssignments = ImmutableMap.builder();
//            ImmutableMap.Builder<Symbol, FunctionHandle> functions = ImmutableMap.builder();
//
//            // 2.a. Rewrite aggregates in terms of pre-projected inputs
//            TranslationMap translations = new TranslationMap(subPlan.getRelationPlan(), analysis);
//            for (FunctionCall aggregate : analysis.getAggregates(node)) {
//                FunctionCall rewritten = (FunctionCall) subPlan.rewrite(aggregate);
//                Symbol newSymbol = symbolAllocator.newSymbol(rewritten, analysis.getType(aggregate));
//
//                aggregationAssignments.put(newSymbol, rewritten);
//                translations.put(aggregate, newSymbol);
//
//                functions.put(newSymbol, analysis.getFunctionInfo(aggregate).getHandle());
//            }
//
//            // 2.b. Rewrite group by expressions in terms of pre-projected inputs
//            Set<Symbol> groupBySymbols = new LinkedHashSet<>();
//            for (FieldOrExpression fieldOrExpression : analysis.getGroupByExpressions(node)) {
//                Symbol symbol = subPlan.translate(fieldOrExpression);
//                groupBySymbols.add(symbol);
//                translations.put(fieldOrExpression, symbol);
//            }
//
//            return new PlanBuilder(translations, new AggregationNode(idAllocator.getNextId(), subPlan.getRoot(), ImmutableList.copyOf(groupBySymbols), aggregationAssignments.build(), functions.build()));
//        }

        @Override
        protected RelationalExpression visitQuerySpecification(QuerySpecification node, PlanningContext context)
        {
            RelationalExpression expression = process(Iterables.getOnlyElement(node.getFrom()), context);

            TranslationMap map = new TranslationMap(analysis, analysis.getOutputDescriptor(Iterables.getOnlyElement(node.getFrom())), "t");

//            analysis.getResolvedNames()
            if (analysis.getWhere(node) != null) {
                RelationalExpression where = map.translate(analysis.getWhere(node));
                expression = new FunctionCall("filter", expression, new Lambda("t", where));
            }

            if (!analysis.getAggregates(node).isEmpty() || !analysis.getGroupByExpressions(node).isEmpty()) {
                // aggregate(relation, group-by-lambda, list<agg + arguments lamba>)
                List<RelationalExpression> groupBy = new ArrayList<>();
                for (FieldOrExpression fieldOrExpression : analysis.getGroupByExpressions(node)) {
                    groupBy.add(map.translate(fieldOrExpression));
                }

                int i = 0;
                List<RelationalExpression> aggregates = new ArrayList<>();
                for (com.facebook.presto.sql.tree.FunctionCall aggregate : analysis.getAggregates(node)) {
                    List<RelationalExpression> translatedArguments = new ArrayList<>();
                    for (Expression argument : aggregate.getArguments()) {
                        RelationalExpression translatedArgument = map.translate(argument);
                        translatedArguments.add(translatedArgument);
                    }
                    Tuple aggregateDefinition = new Tuple(new StringLiteral(aggregate.getName().toString()), new Lambda("t", new Tuple(translatedArguments)));
                    aggregates.add(aggregateDefinition);

                    map.put(aggregate, new FieldRef(new VariableRef("t"), i++));
                }

                List<RelationalExpression> args = new ArrayList<>();
                args.add(expression);
                args.add(new Lambda("t", new Tuple(groupBy)));
                args.addAll(aggregates);

                expression = new FunctionCall("aggregate", args);
            }


            //            expression = group(expression, analysis.getGroupByExpressions(node), context);
//            expression = filter(expression, analysis.getHaving(node), context);

//            List<FieldOrExpression> orderBy = analysis.getOrderByExpressions(node);
//            List<FieldOrExpression> outputs = analysis.getOutputExpressions(node);
//            expression = project(expression, Iterables.concat(orderBy, outputs), context);



            /*
            PlanBuilder builder = planFrom(node);

            Set<InPredicate> inPredicates = analysis.getInPredicates(node);
            builder = appendSemiJoins(builder, inPredicates);

            builder = filter(builder, analysis.getWhere(node));
            builder = aggregate(builder, node);
            builder = filter(builder, analysis.getHaving(node));

            builder = window(builder, node);

            List<FieldOrExpression> orderBy = analysis.getOrderByExpressions(node);
            List<FieldOrExpression> outputs = analysis.getOutputExpressions(node);
            builder = project(builder, Iterables.concat(orderBy, outputs));

            builder = distinct(builder, node, outputs, orderBy);
            builder = sort(builder, node);
            builder = project(builder, analysis.getOutputExpressions(node));
            builder = limit(builder, node);
            */

            return expression;
        }

        private RelationalExpression project(RelationalExpression expression, Iterable<FieldOrExpression> expressions, PlanningContext context)
        {
            ImmutableList.Builder<RelationalExpression> parts = ImmutableList.builder();
            for (FieldOrExpression fieldOrExpression : expressions) {
                if (fieldOrExpression.isFieldReference()) {
                    parts.add(new FieldRef(new VariableRef("t"), fieldOrExpression.getFieldIndex()));
                }
                else {
                    parts.add(process(fieldOrExpression.getExpression(), context));
                }
            }

            // TODO: qualify "t" with context level to get a unique name across closure scope
            return new FunctionCall("project", expression, new Lambda("t", new Tuple(parts.build())));
        }

        private RelationalExpression filter(RelationalExpression expression, Expression condition, PlanningContext context)
        {
            if (condition != null) {
                // TODO: qualify "t" with context level to get a unique name across closure scope
                return new FunctionCall("filter", expression, new Lambda("t", process(condition, context)));
            }

            return expression;
        }

        private RelationalExpression group(RelationalExpression expression, List<FieldOrExpression> expressions, PlanningContext context)
        {
            if (!expressions.isEmpty()) {
                ImmutableList.Builder<RelationalExpression> parts = ImmutableList.builder();
                for (FieldOrExpression fieldOrExpression : expressions) {
                    if (fieldOrExpression.isFieldReference()) {
                        parts.add(new FieldRef(new VariableRef("t"), fieldOrExpression.getFieldIndex()));
                    }
                    else {
                        parts.add(process(fieldOrExpression.getExpression(), context));
                    }
                }

//                context.set
                return new FunctionCall("group", expression, new Lambda("t", new Tuple(parts.build())));
            }

            return expression;
        }

        @Override
        protected RelationalExpression visitTable(Table node, PlanningContext context)
        {
            if (!node.getName().getPrefix().isPresent()) {
                Query namedQuery = analysis.getNamedQuery(node);
                if (namedQuery != null) {
                    // TODO
//                    RelationPlan subPlan = process(namedQuery, null);
//                    return new RelationPlan(subPlan.getRoot(), analysis.getOutputDescriptor(node), subPlan.getOutputSymbols());
                }
            }

            TupleDescriptor descriptor = analysis.getOutputDescriptor(node);
            TableHandle handle = analysis.getTableHandle(node);

            ImmutableList.Builder<ColumnLiteral> columns = ImmutableList.builder();
            for (int i = 0; i < descriptor.getFields().size(); i++) {
                Field field = descriptor.getFields().get(i);
                columns.add(new ColumnLiteral(analysis.getColumn(field)));
            }

            // TODO: partition/implicit predicate

            // call("table", "<table handle>", {"<column handle>", ...})
            return new FunctionCall("table", new TableLiteral(handle), new Tuple(columns.build()));
        }

        @Override
        protected RelationalExpression visitAliasedRelation(AliasedRelation node, PlanningContext context)
        {
            // TODO: sql name -> tuple offset mapping (for planning of callers)
            return process(node.getRelation(), context);
        }

//        @Override
//        protected RelationalExpression visitJoin(Join node, Void context)
//        {
//            // TODO: translate the RIGHT join into a mirrored LEFT join when we refactor (@martint)
//            RelationalExpression left = process(node.getLeft(), context);
//            RelationalExpression right = process(node.getRight(), context);
//
//            List<EquiJoinClause> criteria = analysis.getJoinCriteria(node);
//
//            // Add projections for join criteria
//            PlanBuilder leftPlanBuilder = appendProjections(leftPlan, Iterables.transform(criteria, leftGetter()));
//            PlanBuilder rightPlanBuilder = appendProjections(rightPlan, Iterables.transform(criteria, rightGetter()));
//
//            ImmutableList.Builder<JoinNode.EquiJoinClause> clauses = ImmutableList.builder();
//            for (EquiJoinClause clause : criteria) {
//                Symbol leftSymbol = leftPlanBuilder.translate(clause.getLeft());
//                Symbol rightSymbol = rightPlanBuilder.translate(clause.getRight());
//
//                clauses.add(new JoinNode.EquiJoinClause(leftSymbol, rightSymbol));
//            }
//
//            List<Symbol> outputSymbols = ImmutableList.<Symbol>builder()
//                    .addAll(leftPlan.getOutputSymbols())
//                    .addAll(rightPlan.getOutputSymbols())
//                    .build();
//
//
//            // call("join", "<table handle>", "<table handle">, <condition lambda>)
//            return new RelationPlan(new JoinNode(idAllocator.getNextId(), JoinNode.Type.typeConvert(node.getType()), leftPlanBuilder.getRoot(), rightPlanBuilder.getRoot(), clauses.build()), analysis.getOutputDescriptor(node), outputSymbols);
//        }

        @Override
        protected RelationalExpression visitTableSubquery(TableSubquery node, PlanningContext context)
        {
            return process(node.getQuery(), context);
        }

//        @Override
//        protected RelationPlan visitQuery(Query node, Void context)
//        {
//            PlanBuilder subPlan = new QueryPlanner(analysis, symbolAllocator, idAllocator, metadata, session).process(node, null);
//
//            ImmutableList.Builder<Symbol> outputSymbols = ImmutableList.builder();
//            for (FieldOrExpression fieldOrExpression : analysis.getOutputExpressions(node)) {
//                outputSymbols.add(subPlan.translate(fieldOrExpression));
//            }
//
//            return new RelationPlan(subPlan.getRoot(), analysis.getOutputDescriptor(node),  outputSymbols.build());
//        }

//        @Override
//        protected RelationPlan visitQuerySpecification(QuerySpecification node, Void context)
//        {
//            PlanBuilder subPlan = new QueryPlanner(analysis, symbolAllocator, idAllocator, metadata, session).process(node, null);
//
//            ImmutableList.Builder<Symbol> outputSymbols = ImmutableList.builder();
//            for (FieldOrExpression fieldOrExpression : analysis.getOutputExpressions(node)) {
//                outputSymbols.add(subPlan.translate(fieldOrExpression));
//            }
//
//            return new RelationPlan(subPlan.getRoot(), analysis.getOutputDescriptor(node), outputSymbols.build());
//        }

//        @Override
//        protected RelationPlan visitUnion(Union node, Void context)
//        {
//            checkArgument(!node.getRelations().isEmpty(), "No relations specified for UNION");
//
//            List<Symbol> outputSymbols = null;
//            ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();
//            ImmutableListMultimap.Builder<Symbol, Symbol> symbolMapping = ImmutableListMultimap.builder();
//            for (Relation relation : node.getRelations()) {
//                RelationPlan relationPlan = process(relation, context);
//
//                if (outputSymbols == null) {
//                    // Use the first Relation to derive output symbol names
//                    ImmutableList.Builder<Symbol> outputSymbolBuilder = ImmutableList.builder();
//                    for (Symbol symbol : relationPlan.getOutputSymbols()) {
//                        outputSymbolBuilder.add(symbolAllocator.newSymbol(symbol.getName(), symbolAllocator.getTypes().get(symbol)));
//                    }
//                    outputSymbols = outputSymbolBuilder.build();
//                }
//
//                for (int i = 0; i < outputSymbols.size(); i++) {
//                    symbolMapping.put(outputSymbols.get(i), relationPlan.getOutputSymbols().get(i));
//                }
//
//                sources.add(relationPlan.getRoot());
//            }
//
//            PlanNode planNode = new UnionNode(idAllocator.getNextId(), sources.build(), symbolMapping.build());
//            if (node.isDistinct()) {
//                planNode = distinct(planNode);
//            }
//            return new RelationPlan(planNode, analysis.getOutputDescriptor(node), planNode.getOutputSymbols());
//        }

//        private PlanBuilder appendProjections(RelationPlan subPlan, Iterable<Expression> expressions)
//        {
//            TranslationMap translations = new TranslationMap(subPlan, analysis);
//
//            // Make field->symbol mapping from underlying relation plan available for translations
//            // This makes it possible to rewrite FieldOrExpressions that reference fields from the underlying tuple directly
//            translations.setFieldMappings(subPlan.getOutputSymbols());
//
//            ImmutableMap.Builder<Symbol, Expression> projections = ImmutableMap.builder();
//
//            // add an identity projection for underlying plan
//            for (Symbol symbol : subPlan.getRoot().getOutputSymbols()) {
//                Expression expression = new QualifiedNameReference(symbol.toQualifiedName());
//                projections.put(symbol, expression);
//            }
//
//            ImmutableMap.Builder<Symbol, Expression> newTranslations = ImmutableMap.builder();
//            for (Expression expression : expressions) {
//                Symbol symbol = symbolAllocator.newSymbol(expression, analysis.getType(expression));
//
//                projections.put(symbol, translations.rewrite(expression));
//                newTranslations.put(symbol, expression);
//            }
//            // Now append the new translations into the TranslationMap
//            for (Map.Entry<Symbol, Expression> entry : newTranslations.build().entrySet()) {
//                translations.put(entry.getValue(), entry.getKey());
//            }
//
//            return new PlanBuilder(translations, new ProjectNode(idAllocator.getNextId(), subPlan.getRoot(), projections.build()));
//        }

//        private PlanNode distinct(PlanNode node)
//        {
//            return new AggregationNode(idAllocator.getNextId(),
//                    node,
//                    node.getOutputSymbols(),
//                    ImmutableMap.<Symbol, com.facebook.presto.sql.tree.FunctionCall>of(),
//                    ImmutableMap.<Symbol, FunctionHandle>of());
//        }


        @Override
        protected RelationalExpression visitQualifiedNameReference(QualifiedNameReference node, PlanningContext context)
        {
            Integer input = analysis.getResolvedNames(node).get(node.getName());
            return new FieldRef(new VariableRef(context.getCurrentTupleReference()), input);
        }

        @Override
        protected RelationalExpression visitArithmeticExpression(ArithmeticExpression node, PlanningContext context)
        {
            RelationalExpression left = process(node.getLeft(), context);
            RelationalExpression right = process(node.getRight(), context);

            return new FunctionCall(node.getType().getValue(), left, right);
        }

        @Override
        protected RelationalExpression visitComparisonExpression(ComparisonExpression node, PlanningContext context)
        {
            RelationalExpression left = process(node.getLeft(), context);
            RelationalExpression right = process(node.getRight(), context);

            return new FunctionCall(node.getType().getValue(), left, right);
        }

        @Override
        protected RelationalExpression visitLongLiteral(LongLiteral node, PlanningContext context)
        {
            return new BigintLiteral(node.getValue());
        }

        @Override
        protected RelationalExpression visitFunctionCall(com.facebook.presto.sql.tree.FunctionCall node, PlanningContext context)
        {
            ImmutableList.Builder<RelationalExpression> args = ImmutableList.builder();
            for (Expression arg : node.getArguments()) {
                args.add(process(arg, context));
            }

            return new FunctionCall(node.getName().toString(), args.build());
        }
    }
}
