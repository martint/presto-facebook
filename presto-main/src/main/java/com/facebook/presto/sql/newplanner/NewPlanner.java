package com.facebook.presto.sql.newplanner;

import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.FieldOrExpression;
import com.facebook.presto.sql.analyzer.TupleDescriptor;
import com.facebook.presto.sql.tree.*;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;

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

        @Override
        protected RelationalExpression visitQuerySpecification(QuerySpecification node, PlanningContext context)
        {
            RelationalExpression expression = process(Iterables.getOnlyElement(node.getFrom()), context);

            expression = filter(expression, analysis.getWhere(node), context);
            expression = group(expression, analysis.getGroupByExpressions(node), context);
//            expression = filter(expression, analysis.getHaving(node), context);

            List<FieldOrExpression> orderBy = analysis.getOrderByExpressions(node);
            List<FieldOrExpression> outputs = analysis.getOutputExpressions(node);
            expression = project(expression, Iterables.concat(orderBy, outputs), context);

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
