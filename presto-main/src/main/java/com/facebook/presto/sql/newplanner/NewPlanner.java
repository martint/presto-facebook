package com.facebook.presto.sql.newplanner;

import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.FieldOrExpression;
import com.facebook.presto.sql.analyzer.TupleDescriptor;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class NewPlanner
{
    private Analysis analysis;

    public RelationalExpression plan(Analysis analysis)
    {
        this.analysis = analysis;

        return new Visitor().process(analysis.getQuery(), null);
    }


    class Visitor
        extends DefaultTraversalVisitor<RelationalExpression, Void>
    {
        @Override
        protected RelationalExpression visitNode(Node node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        @Override
        protected RelationalExpression visitQuery(Query query, Void context)
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

        @Override
        protected RelationalExpression visitQuerySpecification(QuerySpecification node, Void context)
        {
            RelationalExpression expression = process(Iterables.getOnlyElement(node.getFrom()), context);

            expression = project(expression, analysis.getOutputExpressions(node));

            return expression;
        }

        private RelationalExpression project(RelationalExpression expression, Iterable<FieldOrExpression> expressions)
        {
            ImmutableList.Builder<RelationalExpression> parts = ImmutableList.builder();

            // TODO:
            //   context should contain
            //      - map of qname -> ref for externally bound names
            //      - map of qname -> input offset for names provided by underlying tuple

            for (FieldOrExpression fieldOrExpression : expressions) {
//                parts.add()
            }

            // TODO: qualify "t" with context level to get a unique name across closure scope
            return new FunctionCall("project", expression, new Lambda("t", new Tuple()));
        }

        @Override
        protected RelationalExpression visitTable(Table node, Void context)
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
            for (Field field : descriptor.getFields()) {
                columns.add(new ColumnLiteral(analysis.getColumn(field)));
            }

            // TODO: partition/implicit predicate
            // TODO: sql name -> tuple offset mapping (for planning of callers)

            // call("table", "<table handle>", {"<column handle>", ...})
            return new FunctionCall("table", new TableLiteral(handle), new Tuple(columns.build()));
        }

        @Override
        protected RelationalExpression visitAliasedRelation(AliasedRelation node, Void context)
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
        protected RelationalExpression visitTableSubquery(TableSubquery node, Void context)
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
    }

}
