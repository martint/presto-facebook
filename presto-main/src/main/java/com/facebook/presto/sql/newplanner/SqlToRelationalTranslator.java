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
import com.facebook.presto.metadata.MetadataUtil;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.AggregateExtractor;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.FieldOrExpression;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.analyzer.TupleDescriptor;
import com.facebook.presto.sql.newplanner.expression.AggregationExpression;
import com.facebook.presto.sql.newplanner.expression.FilterExpression;
import com.facebook.presto.sql.newplanner.expression.JoinExpression;
import com.facebook.presto.sql.newplanner.expression.LimitExpression;
import com.facebook.presto.sql.newplanner.expression.ProjectExpression;
import com.facebook.presto.sql.newplanner.expression.RelationalExpression;
import com.facebook.presto.sql.newplanner.expression.SortExpression;
import com.facebook.presto.sql.newplanner.expression.TableExpression;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.relational.Expressions;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.WithQuery;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_RELATION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_ORDINAL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_CATALOG;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_SCHEMA;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.tree.FunctionCall.argumentsGetter;
import static com.google.common.base.Preconditions.checkArgument;

public class SqlToRelationalTranslator
{
    private final Metadata metadata;
    private final Session session;

    private int nextId;

    public SqlToRelationalTranslator(Metadata metadata, Session session)
    {
        this.metadata = metadata;
        this.session = session;
    }

    public RelationalExpression translate(Statement statement)
    {
        return translate(statement, new Scope()).getExpression();
    }

    // dispatchers
    // TODO: replace with visitor dispatch
    public TranslatedRelationalExpression translate(Statement statement, Scope context)
    {
        if (statement instanceof Query) {
            return translate((Query) statement, context);
        }

        throw new UnsupportedOperationException("not yet implemented");
    }

    private TranslatedRelationalExpression translate(QueryBody body, Scope scope)
    {
        if (body instanceof QuerySpecification) {
            return translate((QuerySpecification) body, scope);
        }
        else if (body instanceof Table) {
            return translate((Table) body, scope);
        }

        throw new UnsupportedOperationException("not yet implemented: " + body.getClass().getName());
    }

    private TranslatedRelationalExpression translate(Relation relation, Scope context)
    {
        if (relation instanceof Join) {
            return translate((Join) relation, context);
        }
        else if (relation instanceof Table) {
            return translate((Table) relation, context);
        }

        throw new UnsupportedOperationException("not yet implemented: " + relation.getClass().getName());
    }



    // =============== translators

    private TranslatedRelationalExpression translate(Query query, Scope scope)
    {
        Scope currentScope = scope;
        if (query.getWith().isPresent()) {
            // for each CTE, translate it and push a new scope with the mapping
            // of name -> (relExpr, (record fields -> ordinals)
            // e.g.,
            //    WITH t AS (SELECT a, a FROM u)
            //
            // produces
            //
            // t -> (project [$0, $0] (table u)), {a: 0, a: 1}
            for (WithQuery withItem : query.getWith().get().getQueries()) {
                TranslatedRelationalExpression translated = translate(withItem.getQuery(), currentScope);

                currentScope = new Scope(currentScope);
                currentScope.register(withItem.getName(), translated);
            }
        }

        TranslatedRelationalExpression body = translate(query.getQueryBody(), currentScope);

        // 1-to-1 mapping of ordinals with underlying relational expression
//        Mappings mappings = new Mappings();
//        for (int i = 0; i < translation.getExpression().getRowType().size(); i++) {
//            mappings.append(i);
//        }

        RelationalExpression result = body.getExpression();

//        Mappings mappings = new Mappings();
//        result = sort(result, query.getOrderBy(), new Scope(currentScope, body.getFields()), mappings);

//        if (query.getLimit().isPresent()) {
//            result = limit(result, query.getLimit().get());
//        }

        return new TranslatedRelationalExpression(body.getFields(), result, body.getBoundVariables());
    }

    private TranslatedRelationalExpression translate(Table table, Scope scope)
    {
        // TODO: resolve name from scope first (CTEs)
        // TODO: resolve views
        QualifiedTableName name = MetadataUtil.createQualifiedTableName(session, table.getName());

        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, name);
        if (!tableHandle.isPresent()) {
            if (!metadata.getCatalogNames().containsKey(name.getCatalogName())) {
                throw new SemanticException(MISSING_CATALOG, table, "Catalog %s does not exist", name.getCatalogName());
            }
            if (!metadata.listSchemaNames(session, name.getCatalogName()).contains(name.getSchemaName())) {
                throw new SemanticException(MISSING_SCHEMA, table, "Schema %s does not exist", name.getSchemaName());
            }

            throw new SemanticException(MISSING_TABLE, table, "Table %s does not exist", name);
        }
        TableMetadata tableMetadata = metadata.getTableMetadata(tableHandle.get());
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(tableHandle.get());

        // TODO: discover columns lazily based on where they are needed (to support datasources that can't enumerate all tables)
        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            Field field = Field.newQualified(table.getName(), Optional.of(column.getName()), column.getType(), column.isHidden());
            fields.add(field);
            ColumnHandle columnHandle = columnHandles.get(column.getName());
            checkArgument(columnHandle != null, "Unknown field %s", field);
        }

        TupleDescriptor descriptor = new TupleDescriptor(fields.build());
        List<Type> types = new ArrayList<>();
        for (Field field : descriptor.getAllFields()) {
            types.add(field.getType());
        }
        return new TranslatedRelationalExpression(descriptor, new TableExpression(nextId(), table.getName(), types), ImmutableList.<ResolvedName>of());
    }


    private TranslatedRelationalExpression translate(Join node, Scope context)
    {
        throw new UnsupportedOperationException("not yet implemented");
//
//        TranslatedRelationalExpression left = translate(node.getLeft(), context);
//        TranslatedRelationalExpression right = translate(node.getRight(), context);
//
//        Sets.SetView<QualifiedName> duplicateAliases = Sets.intersection(left.getFields().getRelationAliases(), right.getFields().getRelationAliases());
//        if (!duplicateAliases.isEmpty()) {
//            throw new SemanticException(DUPLICATE_RELATION, node, "Relations appear more than once: %s", duplicateAliases);
//        }
//
//        TupleDescriptor outputDescriptor = left.getFields().joinWith(right.getFields());
//
//        RowExpression condition;
//        if (node.getType() == Join.Type.CROSS) {
//            condition = Expressions.constant(true, BooleanType.BOOLEAN);
//        }
//        else {
//            Scope joinContext = new Scope(context, outputDescriptor);
//            // TODO: translate join condition
//            condition = null;
//        }
//
//        return new TranslatedRelationalExpression(outputDescriptor,
//                new JoinExpression(nextId(), JoinNode.Type.typeConvert(node.getType()), left.getExpression(), right.getExpression(), condition), boundVariables);
    }

    public TranslatedRelationalExpression translate(QuerySpecification query, Scope parentScope)
    {
        // TODO: introduce FromNode and support for implicit cross joins
        TranslatedRelationalExpression from = translate(Iterables.getOnlyElement(query.getFrom()), parentScope);

        Scope scope = new Scope(parentScope, from.getFields());


        // TODO:
        //     1. extract all name references & subqueries
        //     2. add projection for all references
        //     3. add filter rewritten using projection
        //     4. if agg
        //           a. add projection for group by keys + agg arguments
        //           b. add aggregate calls
        //           c. add projection for agg expressions
        //           d. add filter for rewritten having clause
        //     5. add windows
        //     6. add project for order by expression
        //     7. add distinct
        //     8. add order by
        //     9. add project for outputs
        //    10. add limit



        // analyze where clause
        if (query.getWhere().isPresent()) {
            TranslatedRowExpression where = translate(query.getWhere().get(), scope);
        }


        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(scope, this);

        // analyze select expressions
        for (SelectItem item : query.getSelect().getSelectItems()) {
            if (item instanceof SingleColumn) {
                analyzer.analyze(((SingleColumn) item).getExpression());
            }
        }

        // analyze group-by expressions
        for (Expression expression : query.getGroupBy()) {
            if (!(expression instanceof LongLiteral)) { // ordinal reference
                analyzer.analyze(expression);
            }
        }

        // analyze order-by expressions
        for (SortItem item : query.getOrderBy()) {
            if (!(item.getSortKey() instanceof LongLiteral)) { // ordinal reference
                analyzer.analyze(item.getSortKey());
            }
        }

        // analyze having expression
        if (query.getHaving().isPresent()) {
            analyzer.analyze(query.getHaving().get());
        }

        RelationalExpression result = from.getExpression();

        if (query.getWhere().isPresent()) {
            result = filter(from.getExpression(), translate(query.getWhere().get(), scope));
        }

        // compute expanded select expressions and make sure they reference valid fields
        //    compute output descriptor

//        result = aggregate(result, query, scope);

        // add filter for having clause and rewrite in terms of outputs of parent
        // add window exprs and rewrite in terms of outputs of parent
        // project output + order by expressions and rewrite in terms of outputs of parent
        // add distinct
        // add order by and rewrite in terms of outputs of parent
        // project outputs and rewrite in terms of outputs of parent

//        if (query.getLimit().isPresent()) {
//            result = limit(result, query.getLimit().get());
//        }

        TupleDescriptor outputDescriptor = from.getFields(); // TODO: compute output descriptor
        return new TranslatedRelationalExpression(outputDescriptor, result, boundVariables);

        /*
        List<FieldOrExpression> outputExpressions = analyzeSelect(node, tupleDescriptor, scope);
        List<FieldOrExpression> groupByExpressions = analyzeGroupBy(node, tupleDescriptor, scope, outputExpressions);
        List<FieldOrExpression> orderByExpressions = analyzeOrderBy(node, tupleDescriptor, scope, outputExpressions);
        analyzeHaving(node, tupleDescriptor, scope);

        analyzeAggregations(node, tupleDescriptor, groupByExpressions, outputExpressions, orderByExpressions, scope);
        analyzeWindowFunctions(node, outputExpressions, orderByExpressions);

        TupleDescriptor descriptor = computeOutputDescriptor(node, tupleDescriptor);
        analysis.setOutputDescriptor(node, descriptor);

    */

    }

    private RelationalExpression aggregate(RelationalExpression input, QuerySpecification query, Scope context)
    {
        // TODO: validate aggregation semantics
        // Gather all aggregates
        // add groupby/agg expr & translate in terms of FROM

        ImmutableList.Builder<Expression> expressions = ImmutableList.builder();
        for (SelectItem item : query.getSelect().getSelectItems()) {
            if (item instanceof SingleColumn) {
                expressions.add(((SingleColumn) item).getExpression());
            }
        }
        for (SortItem item : query.getOrderBy()) {
            expressions.add(item.getSortKey());
        }
        if (query.getHaving().isPresent()) {
            expressions.add(query.getHaving().get());
        }

        List<FunctionCall> aggregates = extractAggregates(expressions.build());

        if (aggregates.isEmpty() && query.getGroupBy().isEmpty()) {
            return input;
        }

        RelationalExpression result = input;

        // 1. pre-project all scalar inputs (arguments and non-trivial group by expressions)
        Set<Expression> arguments = IterableTransformer.on(aggregates)
                .transformAndFlatten(argumentsGetter())
                .set();

        Iterable<Expression> inputs = Iterables.concat(query.getGroupBy(), arguments);
        result = project(result, inputs, context);


        // 2. aggregate
        // aggregation expression produces outputs of the following form
        //  [ g1, g2, ..., gN, agg1, agg2, ... aggN ]

        // 2.a. Rewrite group by expressions in terms of pre-projected inputs
        Map<Expression, Integer> mappings = new HashMap<>();
        int i = 0;
        ImmutableList.Builder<RowExpression> groupByExpressions = ImmutableList.builder();
        for (Expression expression : query.getGroupBy()) { // TODO: set?
            groupByExpressions.add(translate(expression, context));
            mappings.put(expression, i);
            i++;
        }

        // 2.b. Rewrite aggregates in terms of pre-projected inputs
        ImmutableList.Builder<RowExpression> aggregateExpressions = ImmutableList.builder();
        for (FunctionCall aggregate : aggregates) { // TODO: set?
            aggregateExpressions.add((translate(aggregate, context)));
            mappings.put(aggregate, i);
            i++;
        }

        result = new AggregationExpression(nextId(), result, groupByExpressions.build(), aggregateExpressions.build());

        return result;
    }

    private RelationalExpression project(RelationalExpression input, Iterable<Expression> projections, Scope context)
    {
        ImmutableList.Builder<RowExpression> translated = ImmutableList.builder();

        Map<Expression, Integer> mappings = new HashMap<>();
        int i = 0;
        for (Expression projection : projections) { // TODO: set?
            translated.add(translate(projection, context));
            mappings.put(projection, i);
            i++;
        }

        // TODO: return mappings so that translator can rewrite expressions
        return new ProjectExpression(nextId(), input, translated.build());
    }

    private List<FunctionCall> extractAggregates(Iterable<Expression> expressions)
    {
        AggregateExtractor extractor = new AggregateExtractor(metadata);
        for (Expression expression : expressions) {
            extractor.process(expression, null);
        }
        return extractor.getAggregates();
    }

    private TupleDescriptor computeOutputDescriptor(List<SelectItem> select, TupleDescriptor inputTupleDescriptor)
    {
        ImmutableList.Builder<Field> outputFields = ImmutableList.builder();

        int index = 0;
        for (SelectItem item : select) {
            if (item instanceof AllColumns) {
                // expand * and T.*
                Optional<QualifiedName> starPrefix = ((AllColumns) item).getPrefix();

                for (Field field : inputTupleDescriptor.resolveFieldsWithPrefix(starPrefix)) {
                    outputFields.add(Field.newUnqualified(field.getName(), field.getType()));
                    index++;
                }
            }
            else if (item instanceof SingleColumn) {
                SingleColumn column = (SingleColumn) item;

                Optional<String> alias = column.getAlias();
                if (!alias.isPresent() && column.getExpression() instanceof QualifiedNameReference) {
                    alias = Optional.of(((QualifiedNameReference) column.getExpression()).getName().getSuffix());
                }

//                outputFields.add(Field.newUnqualified(alias, inputTupleDescriptor.));
                index++;
            }
            else {
                throw new IllegalArgumentException("Unsupported SelectItem type: " + item.getClass().getName());
            }
        }

        return new TupleDescriptor(outputFields.build());
    }

    private TranslatedRowExpression translate(Expression expression, Scope scope)
    {
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(scope, this);
        analyzer.analyze(expression);

        ExpressionAnalysis analysis = analyzer.getResult();

        // TODO: introduce "let" if necessary (i.e., there are bound variables for the current scope)
        // TODO: translate expression
    }

    private RelationalExpression filter(RelationalExpression expression, RowExpression filter)
    {
        return new FilterExpression(nextId(), expression, filter);
    }

    private RelationalExpression limit(RelationalExpression expression, String limit)
    {
        return new LimitExpression(nextId(), expression, Long.valueOf(limit));
    }

    private RelationalExpression sort(RelationalExpression input, List<SortItem> orderBy, Scope scope)
    {
        if (orderBy.isEmpty()) {
            return input;
        }

        List<Object> sortItems = new ArrayList<>();

        for (SortItem item : orderBy) {
            FieldOrExpression fieldOrExpression = resolveOrdinal(scope.getTupleDescriptor(), item.getSortKey(), "ORDER BY");
//            RowExpression translated = translate(fieldOrExpression, mappings);
        }

        return new SortExpression(nextId(), input);
    }

    private FieldOrExpression resolveOrdinal(TupleDescriptor descriptor, Expression expression, String message)
    {
        if (expression instanceof LongLiteral) {
            int ordinal = (int) ((LongLiteral) expression).getValue();
            if (ordinal < 1 || ordinal > descriptor.getVisibleFieldCount()) {
                throw new SemanticException(INVALID_ORDINAL, expression, "%s position %s is not in select list", message, ordinal);
            }

            return new FieldOrExpression(ordinal - 1);
        }
        else {
            return new FieldOrExpression(expression);
        }
    }

    private RowExpression translate(FieldOrExpression fieldOrExpression)
    {
        return null;
//        if (fieldOrExpression.isFieldReference()) {
//            // TODO: need access to Scope to determine field type
//            return Expressions.field(mappings.getOffset(fieldOrExpression.getFieldIndex(), ....);
//        }
//        else {
//            // translate expression
//            // when qname is encountered,
//            //     resolve first against tupledescriptor in current scope to determine field. If found, translate field index based on current field->slot mapping
//            //     if not found in descriptor, resolve against parent scope. If found, translate to VariableReference and register binding to field in corresponding scope (how to find slot it maps to?)
//        }
//
    }


    private int nextId()
    {
        return nextId++;
    }
}
