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
package com.facebook.presto.sql.parser2;

import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.Approximate;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.DropView;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.ExplainFormat;
import com.facebook.presto.sql.tree.ExplainOption;
import com.facebook.presto.sql.tree.ExplainType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Extract;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.JoinUsing;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NaturalJoin;
import com.facebook.presto.sql.tree.NegativeExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.RenameTable;
import com.facebook.presto.sql.tree.ResetSession;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.sql.tree.ShowCatalogs;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.ShowFunctions;
import com.facebook.presto.sql.tree.ShowPartitions;
import com.facebook.presto.sql.tree.ShowSchemas;
import com.facebook.presto.sql.tree.ShowSession;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Unnest;
import com.facebook.presto.sql.tree.Use;
import com.facebook.presto.sql.tree.Values;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.WindowFrame;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.collect.ImmutableList;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

// TODO: TestSqlParser.testParseErrorStartOfLine should report "expected {AS, identifier, WHERE}"
// TODO: improve error message for TestSqlParser.testTokenizeErrorIncompleteToken
// TODO: preserve + in unary arithmetic expression
public class AstBuilder
        extends SqlBaseVisitor<Node>
{
    @Override
    public Node visitSingleStatement(@NotNull SqlParser.SingleStatementContext ctx)
    {
        return visit(ctx.statement());
    }

    @Override
    public Node visitSingleExpression(@NotNull SqlParser.SingleExpressionContext ctx)
    {
        return visit(ctx.expression());
    }

    // statements

    @Override
    public Node visitUse(@NotNull SqlParser.UseContext ctx)
    {
        return new Use(getTextIfPresent(ctx.catalog), ctx.schema.getText());
    }

    @Override
    public Node visitCreateTableAsSelect(@NotNull SqlParser.CreateTableAsSelectContext ctx)
    {
        return new CreateTable(getQualifiedName(ctx.qualifiedName()), (Query) visit(ctx.query()));
    }

    @Override
    public Node visitDropTable(@NotNull SqlParser.DropTableContext ctx)
    {
        return new DropTable(getQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public Node visitDropView(@NotNull SqlParser.DropViewContext ctx)
    {
        return new DropView(getQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public Node visitInsertInto(@NotNull SqlParser.InsertIntoContext ctx)
    {
        return new Insert(getQualifiedName(ctx.qualifiedName()), (Query) visit(ctx.query()));
    }

    @Override
    public Node visitRenameTable(@NotNull SqlParser.RenameTableContext ctx)
    {
        return new RenameTable(getQualifiedName(ctx.from), getQualifiedName(ctx.to));
    }

    @Override
    public Node visitCreateView(@NotNull SqlParser.CreateViewContext ctx)
    {
        boolean replace = ctx.REPLACE() != null;
        return new CreateView(getQualifiedName(ctx.qualifiedName()), (Query) visit(ctx.query()), replace);
    }

    // query expressions

    @Override
    public Node visitQuery(@NotNull SqlParser.QueryContext ctx)
    {
        Optional<With> with = Optional.ofNullable(ctx.with())
                .map(this::visit)
                .map(With.class::cast);

        Query body = (Query) visit(ctx.queryNoWith());

        return new Query(with,
                body.getQueryBody(),
                body.getOrderBy(),
                body.getLimit(),
                body.getApproximate());
    }

    @Override
    public Node visitWith(@NotNull SqlParser.WithContext ctx)
    {
        List<WithQuery> namedQueries = ctx.namedQuery().stream()
                .map(this::visit)
                .map(WithQuery.class::cast)
                .collect(Collectors.toList());

        return new With(ctx.RECURSIVE() != null, namedQueries);
    }

    @Override
    public Node visitNamedQuery(@NotNull SqlParser.NamedQueryContext ctx)
    {
        return new WithQuery(ctx.name.getText(), (Query) visit(ctx.query()), getColumnAliases(ctx.columnAliases()));
    }

    @Override
    public Node visitQueryNoWith(@NotNull SqlParser.QueryNoWithContext ctx)
    {
        QueryBody term = (QueryBody) visit(ctx.queryTerm());

        List<SortItem> orderBy = ctx.sortItem().stream()
                .map(this::visit)
                .map(SortItem.class::cast)
                .collect(Collectors.toList());

        Optional<Approximate> approximate = getTextIfPresent(ctx.confidence)
                .map(Approximate::new);

        if (term instanceof QuerySpecification) {
            // When we have a simple query specification
            // followed by order by limit, fold the order by and limit
            // clauses into the query specification (analyzer/planner
            // expects this structure to resolve references with respect
            // to columns defined in the query specification)
            QuerySpecification query = (QuerySpecification) term;

            return new Query(
                    Optional.<With>empty(),
                    new QuerySpecification(
                            query.getSelect(),
                            query.getFrom(),
                            query.getWhere(),
                            query.getGroupBy(),
                            query.getHaving(),
                            orderBy,
                            getTextIfPresent(ctx.limit)),
                    ImmutableList.of(),
                    Optional.<String>empty(),
                    approximate);
        }

        return new Query(Optional.<With>empty(), term, orderBy, getTextIfPresent(ctx.limit), approximate);
    }

    @Override
    public Node visitQuerySpecification(@NotNull SqlParser.QuerySpecificationContext ctx)
    {
        List<SelectItem> selectItems = ctx.selectItem().stream()
                .map(this::visit)
                .map(SelectItem.class::cast)
                .collect(Collectors.toList());

        Select select = new Select(isDistinct(ctx.setQuantifier()), selectItems);

        List<Relation> relations = ctx.relation().stream()
                .map(this::visit)
                .map(Relation.class::cast)
                .collect(Collectors.toList());

        Optional<Relation> from = Optional.empty();

        if (!relations.isEmpty()) {
            // synthesize implicit join nodes
            Iterator<Relation> iterator = relations.iterator();
            Relation relation = iterator.next();

            while (iterator.hasNext()) {
                relation = new Join(Join.Type.IMPLICIT, relation, iterator.next(), Optional.<JoinCriteria>empty());
            }

            from = Optional.of(relation);
        }

        return new QuerySpecification(select, from, visitIfPresent(ctx.where), visitExpressions(ctx.groupBy), visitIfPresent(ctx.having), ImmutableList.of(), Optional.<String>empty());
    }

    @Override
    public Node visitSetOperation(@NotNull SqlParser.SetOperationContext ctx)
    {
        QueryBody left = (QueryBody) visit(ctx.left);
        QueryBody right = (QueryBody) visit(ctx.right);

        boolean distinct = ctx.setQuantifier() == null || ctx.setQuantifier().DISTINCT() != null;

        switch (ctx.operator.getType()) {
            case SqlLexer.UNION:
                return new Union(ImmutableList.of(left, right), distinct);
            case SqlLexer.INTERSECT:
                return new Intersect(ImmutableList.of(left, right), distinct);
            case SqlLexer.EXCEPT:
                return new Except(left, right, distinct);
        }

        throw new UnsupportedOperationException("Unsupported set operation: " + ctx.operator.getText());
    }

    @Override
    public Node visitSelectAll(@NotNull SqlParser.SelectAllContext ctx)
    {
        if (ctx.qualifiedName() != null) {
            return new AllColumns(getQualifiedName(ctx.qualifiedName()));
        }

        return new AllColumns();
    }

    @Override
    public Node visitSelectSingle(@NotNull SqlParser.SelectSingleContext ctx)
    {
        Optional<String> alias = getTextIfPresent(ctx.identifier());

        return new SingleColumn((Expression) visit(ctx.expression()), alias);
    }

    @Override
    public Node visitTable(@NotNull SqlParser.TableContext ctx)
    {
        return new Table(getQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public Node visitSubquery(@NotNull SqlParser.SubqueryContext ctx)
    {
        return new TableSubquery((Query) visit(ctx.queryNoWith()));
    }

    @Override
    public Node visitInlineTable(@NotNull SqlParser.InlineTableContext ctx)
    {
        List<Row> rows = ctx.rowValue().stream()
                .map(this::visit)
                .map(Row.class::cast)
                .collect(Collectors.toList());

        return new Values(rows);
    }

    @Override
    public Node visitRowValue(@NotNull SqlParser.RowValueContext ctx)
    {
        return new Row(visitExpressions(ctx.expression()));
    }

    @Override
    public Node visitExplain(@NotNull SqlParser.ExplainContext ctx)
    {
        List<ExplainOption> options = ctx.explainOption().stream()
                .map(this::visit)
                .map(ExplainOption.class::cast)
                .collect(Collectors.toList());

        return new Explain((Statement) visit(ctx.statement()), options);
    }

    @Override
    public Node visitExplainFormat(@NotNull SqlParser.ExplainFormatContext ctx)
    {
        ExplainFormat.Type type;
        switch (ctx.value.getType()) {
            case SqlLexer.GRAPHVIZ:
                type = ExplainFormat.Type.GRAPHVIZ;
                break;
            case SqlLexer.TEXT:
                type = ExplainFormat.Type.TEXT;
                break;
            case SqlLexer.JSON:
                type = ExplainFormat.Type.JSON;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported EXPLAIN format: " + ctx.value.getText());
        }

        return new ExplainFormat(type);
    }

    @Override
    public Node visitExplainType(@NotNull SqlParser.ExplainTypeContext ctx)
    {
        ExplainType.Type type;
        switch (ctx.value.getType()) {
            case SqlLexer.LOGICAL:
                type = ExplainType.Type.LOGICAL;
                break;
            case SqlLexer.DISTRIBUTED:
                type = ExplainType.Type.DISTRIBUTED;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported EXPLAIN type: " + ctx.value.getText());
        }

        return new ExplainType(type);
    }

    @Override
    public Node visitShowTables(@NotNull SqlParser.ShowTablesContext ctx)
    {
        QualifiedName schema = Optional.ofNullable(ctx.qualifiedName())
                .map(AstBuilder::getQualifiedName)
                .orElse(null);

        String pattern = getTextIfPresent(ctx.pattern)
                .map(AstBuilder::unquote)
                .orElse(null);

        return new ShowTables(schema, pattern);
    }

    @Override
    public Node visitShowSchemas(@NotNull SqlParser.ShowSchemasContext ctx)
    {
        Optional<String> catalog = getTextIfPresent(ctx.identifier());

        return new ShowSchemas(catalog);
    }

    @Override
    public Node visitShowCatalogs(@NotNull SqlParser.ShowCatalogsContext ctx)
    {
        return new ShowCatalogs();
    }

    @Override
    public Node visitShowColumns(@NotNull SqlParser.ShowColumnsContext ctx)
    {
        return new ShowColumns(getQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public Node visitShowPartitions(@NotNull SqlParser.ShowPartitionsContext ctx)
    {
        Optional<Expression> where = visitIfPresent(ctx.booleanExpression());

        List<SortItem> orderBy = ctx.sortItem().stream()
                .map(this::visit)
                .map(SortItem.class::cast)
                .collect(Collectors.toList());

        return new ShowPartitions(getQualifiedName(ctx.qualifiedName()), where, orderBy, getTextIfPresent(ctx.limit));
    }

    @Override
    public Node visitShowFunctions(@NotNull SqlParser.ShowFunctionsContext ctx)
    {
        return new ShowFunctions();
    }

    @Override
    public Node visitShowSession(@NotNull SqlParser.ShowSessionContext ctx)
    {
        return new ShowSession();
    }

    @Override
    public Node visitSetSession(@NotNull SqlParser.SetSessionContext ctx)
    {
        return new SetSession(getQualifiedName(ctx.qualifiedName()), unquote(ctx.STRING().getText()));
    }

    @Override
    public Node visitResetSession(@NotNull SqlParser.ResetSessionContext ctx)
    {
        return new ResetSession(getQualifiedName(ctx.qualifiedName()));
    }

    // boolean expressions

    @Override
    public Node visitLogicalNot(@NotNull SqlParser.LogicalNotContext ctx)
    {
        return new NotExpression((Expression) visit(ctx.booleanExpression()));
    }

    @Override
    public Node visitLogicalBinary(@NotNull SqlParser.LogicalBinaryContext ctx)
    {
        LogicalBinaryExpression.Type type;

        switch (ctx.operator.getType()) {
            case SqlLexer.AND:
                type = LogicalBinaryExpression.Type.AND;
                break;
            case SqlLexer.OR:
                type = LogicalBinaryExpression.Type.OR;
                break;
            default:
                throw new IllegalArgumentException("Unsupported operator: " + ctx.operator.getText());
        }

        return new LogicalBinaryExpression(
                type,
                (Expression) visit(ctx.left),
                (Expression) visit(ctx.right));
    }

    // from clause

    @Override
    public Node visitJoinRelation(@NotNull SqlParser.JoinRelationContext ctx)
    {
        Relation left = (Relation) visit(ctx.left);
        Relation right = (Relation) visit(ctx.right);

        if (ctx.CROSS() != null) {
            return new Join(Join.Type.CROSS, left, right, Optional.<JoinCriteria>empty());
        }

        JoinCriteria criteria;
        if (ctx.NATURAL() != null) {
            criteria = new NaturalJoin();
        }
        else if (ctx.joinCriteria().ON() != null) {
            criteria = new JoinOn((Expression) visit(ctx.joinCriteria().booleanExpression()));
        }
        else if (ctx.joinCriteria().USING() != null) {
            List<String> columns = ctx.joinCriteria()
                    .identifier().stream()
                    .map(ParseTree::getText)
                    .collect(Collectors.toList());

            criteria = new JoinUsing(columns);
        }
        else {
            throw new UnsupportedOperationException("Unsupported join criteria");
        }

        Join.Type joinType;
        if (ctx.joinType().LEFT() != null) {
            joinType = Join.Type.LEFT;
        }
        else if (ctx.joinType().RIGHT() != null) {
            joinType = Join.Type.RIGHT;
        }
        else if (ctx.joinType().FULL() != null) {
            joinType = Join.Type.FULL;
        }
        else {
            joinType = Join.Type.INNER;
        }

        return new Join(joinType, left, right, Optional.of(criteria));
    }

    @Override
    public Node visitSampledRelation(@NotNull SqlParser.SampledRelationContext ctx)
    {
        Relation child = (Relation) visit(ctx.aliasedRelation());

        if (ctx.TABLESAMPLE() == null) {
            return child;
        }

        SampledRelation.Type type;
        Token token = (Token) ctx.sampleType().getChild(0).getPayload();
        switch (token.getType()) {
            case SqlLexer.BERNOULLI:
                type = SampledRelation.Type.BERNOULLI;
                break;
            case SqlLexer.SYSTEM:
                type = SampledRelation.Type.SYSTEM;
                break;
            case SqlLexer.POISSONIZED:
                type = SampledRelation.Type.POISSONIZED;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported sampling method: " + token.getText());
        }

        Optional<List<Expression>> stratifyOn = Optional.empty();
        if (ctx.STRATIFY() != null) {
            stratifyOn = Optional.of(visitExpressions(ctx.stratify));
        }

        return new SampledRelation(
                child,
                type,
                (Expression) visit(ctx.percentage),
                ctx.RESCALED() != null,
                stratifyOn);
    }

    @Override
    public Node visitAliasedRelation(@NotNull SqlParser.AliasedRelationContext ctx)
    {
        Relation child = (Relation) visit(ctx.relationPrimary());

        if (ctx.identifier() == null) {
            return child;
        }

        return new AliasedRelation(child, ctx.identifier().getText(), getColumnAliases(ctx.columnAliases()));
    }

    @Override
    public Node visitTableName(@NotNull SqlParser.TableNameContext ctx)
    {
        return new Table(getQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public Node visitSubqueryRelation(@NotNull SqlParser.SubqueryRelationContext ctx)
    {
        return new TableSubquery((Query) visit(ctx.query()));
    }

    @Override
    public Node visitUnnest(@NotNull SqlParser.UnnestContext ctx)
    {
        return new Unnest(visitExpressions(ctx.expression()));
    }

    @Override
    public Node visitParenthesizedRelation(@NotNull SqlParser.ParenthesizedRelationContext ctx)
    {
        return visit(ctx.relation());
    }

// predicates

    @Override
    public Node visitComparison(@NotNull SqlParser.ComparisonContext ctx)
    {
        ComparisonExpression.Type type;

        TerminalNode operator = (TerminalNode) ctx.comparisonOperator().getChild(0);
        switch (operator.getSymbol().getType()) {
            case SqlLexer.EQ:
                type = ComparisonExpression.Type.EQUAL;
                break;
            case SqlLexer.NEQ:
                type = ComparisonExpression.Type.NOT_EQUAL;
                break;
            case SqlLexer.LT:
                type = ComparisonExpression.Type.LESS_THAN;
                break;
            case SqlLexer.LTE:
                type = ComparisonExpression.Type.LESS_THAN_OR_EQUAL;
                break;
            case SqlLexer.GT:
                type = ComparisonExpression.Type.GREATER_THAN;
                break;
            case SqlLexer.GTE:
                type = ComparisonExpression.Type.GREATER_THAN_OR_EQUAL;
                break;
            default:
                throw new IllegalArgumentException("Unsupported operator: " + operator.getSymbol().getText());
        }

        return new ComparisonExpression(type, (Expression) visit(ctx.left), (Expression) visit(ctx.right));
    }

    @Override
    public Node visitDistinctFrom(@NotNull SqlParser.DistinctFromContext ctx)
    {
        Expression expression = new ComparisonExpression(
                ComparisonExpression.Type.IS_DISTINCT_FROM,
                (Expression) visit(ctx.left),
                (Expression) visit(ctx.right));

        if (ctx.NOT() != null) {
            expression = new NotExpression(expression);
        }

        return expression;
    }

    @Override
    public Node visitBetween(@NotNull SqlParser.BetweenContext ctx)
    {
        return new BetweenPredicate(
                (Expression) visit(ctx.value),
                (Expression) visit(ctx.lower),
                (Expression) visit(ctx.upper));
    }

    @Override
    public Node visitNullPredicate(@NotNull SqlParser.NullPredicateContext ctx)
    {
        Expression child = (Expression) visit(ctx.value);

        if (ctx.NOT() == null) {
            return new IsNullPredicate(child);
        }

        return new IsNotNullPredicate(child);
    }

    @Override
    public Node visitLike(@NotNull SqlParser.LikeContext ctx)
    {
        Expression escape = null;
        if (ctx.escape != null) {
            escape = (Expression) visit(ctx.escape);
        }

        Expression result = new LikePredicate((Expression) visit(ctx.value), (Expression) visit(ctx.pattern), escape);

        if (ctx.NOT() != null) {
            result = new NotExpression(result);
        }

        return result;
    }

    @Override
    public Node visitInList(@NotNull SqlParser.InListContext ctx)
    {
        return new InPredicate(
                (Expression) visit(ctx.valueExpression()),
                new InListExpression(visitExpressions(ctx.expression())));
    }

    @Override
    public Node visitInSubquery(@NotNull SqlParser.InSubqueryContext ctx)
    {
        Expression result = new InPredicate(
                (Expression) visit(ctx.valueExpression()),
                new SubqueryExpression((Query) visit(ctx.query())));

        if (ctx.NOT() != null) {
            result = new NotExpression(result);
        }

        return result;
    }

// ************** value expressions **************

    @Override
    public Node visitArithmeticUnary(@NotNull SqlParser.ArithmeticUnaryContext ctx)
    {
        Expression result = (Expression) visit(ctx.valueExpression());

        if (ctx.operator.getType() == SqlLexer.MINUS) {
            result = new NegativeExpression(result);
        }

        return result;
    }

    @Override
    public Node visitArithmeticBinary(@NotNull SqlParser.ArithmeticBinaryContext ctx)
    {
        ArithmeticExpression.Type type;

        switch (ctx.operator.getType()) {
            case SqlLexer.PLUS:
                type = ArithmeticExpression.Type.ADD;
                break;
            case SqlLexer.MINUS:
                type = ArithmeticExpression.Type.SUBTRACT;
                break;
            case SqlLexer.ASTERISK:
                type = ArithmeticExpression.Type.MULTIPLY;
                break;
            case SqlLexer.SLASH:
                type = ArithmeticExpression.Type.DIVIDE;
                break;
            case SqlLexer.PERCENT:
                type = ArithmeticExpression.Type.MODULUS;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operator: " + ctx.operator.getText());
        }

        return new ArithmeticExpression(
                type,
                (Expression) visit(ctx.left),
                (Expression) visit(ctx.right));
    }

    @Override
    public Node visitConcatenation(@NotNull SqlParser.ConcatenationContext ctx)
    {
        return new FunctionCall(new QualifiedName("concat"), ImmutableList.of(
                (Expression) visit(ctx.left),
                (Expression) visit(ctx.right)));
    }

    // primary expressions

    @Override
    public Node visitSubExpression(@NotNull SqlParser.SubExpressionContext ctx)
    {
        return visit(ctx.expression());
    }

    @Override
    public Node visitArrayConstructor(@NotNull SqlParser.ArrayConstructorContext ctx)
    {
        return new ArrayConstructor(visitExpressions(ctx.expression()));
    }

    @Override
    public Node visitCast(@NotNull SqlParser.CastContext ctx)
    {
        boolean isTryCast = ctx.TRY_CAST() != null;
        return new Cast((Expression) visit(ctx.expression()), ctx.type().getText(), isTryCast);
    }

    @Override
    public Node visitSpecialDateTimeFunction(@NotNull SqlParser.SpecialDateTimeFunctionContext ctx)
    {
        CurrentTime.Type type;

        switch (ctx.name.getType()) {
            case SqlLexer.CURRENT_DATE:
                type = CurrentTime.Type.DATE;
                break;
            case SqlLexer.CURRENT_TIME:
                type = CurrentTime.Type.TIME;
                break;
            case SqlLexer.CURRENT_TIMESTAMP:
                type = CurrentTime.Type.TIMESTAMP;
                break;
            case SqlLexer.LOCALTIME:
                type = CurrentTime.Type.LOCALTIME;
                break;
            case SqlLexer.LOCALTIMESTAMP:
                type = CurrentTime.Type.LOCALTIMESTAMP;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported special function: " + ctx.name.getText());
        }

        if (ctx.precision != null) {
            return new CurrentTime(type, Integer.parseInt(ctx.precision.getText()));
        }

        return new CurrentTime(type);
    }

    @Override
    public Node visitExtract(@NotNull SqlParser.ExtractContext ctx)
    {
        return new Extract((Expression) visit(ctx.valueExpression()), Extract.Field.valueOf(ctx.identifier().getText().toUpperCase()));
    }

    @Override
    public Node visitSubstring(@NotNull SqlParser.SubstringContext ctx)
    {
        return new FunctionCall(new QualifiedName("substr"), visitExpressions(ctx.valueExpression()));
    }

    @Override
    public Node visitSubscript(@NotNull SqlParser.SubscriptContext ctx)
    {
        return new SubscriptExpression((Expression) visit(ctx.value), (Expression) visit(ctx.index));
    }

    @Override
    public Node visitSubqueryExpression(@NotNull SqlParser.SubqueryExpressionContext ctx)
    {
        return new SubqueryExpression((Query) visit(ctx.query()));
    }

    @Override
    public Node visitColumnReference(@NotNull SqlParser.ColumnReferenceContext ctx)
    {
        return new QualifiedNameReference(getQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public Node visitSimpleCase(@NotNull SqlParser.SimpleCaseContext ctx)
    {
        List<WhenClause> whenClauses = ctx.whenClause().stream()
                .map(this::visit)
                .map(WhenClause.class::cast)
                .collect(Collectors.toList());

        Expression elseClause = visitIfPresent(ctx.elseExpression)
                .orElse(null);

        return new SimpleCaseExpression((Expression) visit(ctx.valueExpression()), whenClauses, elseClause);
    }

    @Override
    public Node visitSearchedCase(@NotNull SqlParser.SearchedCaseContext ctx)
    {
        List<WhenClause> whenClauses = ctx.whenClause().stream()
                .map(this::visit)
                .map(WhenClause.class::cast)
                .collect(Collectors.toList());

        Expression elseClause = visitIfPresent(ctx.elseExpression)
                .orElse(null);

        return new SearchedCaseExpression(whenClauses, elseClause);
    }

    @Override
    public Node visitWhenClause(@NotNull SqlParser.WhenClauseContext ctx)
    {
        return new WhenClause((Expression) visit(ctx.booleanExpression()), (Expression) visit(ctx.expression()));
    }

    @Override
    public Node visitFunctionCall(@NotNull SqlParser.FunctionCallContext ctx)
    {
        boolean distinct = isDistinct(ctx.setQuantifier());

        Window window = Optional.ofNullable(ctx.over())
                .map(this::visit)
                .map(Window.class::cast)
                .orElse(null);

        return new FunctionCall(getQualifiedName(ctx.qualifiedName()), window, distinct, visitExpressions(ctx.expression()));
    }

    @Override
    public Node visitScalarFunctionCall(@NotNull SqlParser.ScalarFunctionCallContext ctx)
    {
        QualifiedName name = getQualifiedName(ctx.qualifiedName());

        if (name.toString().equalsIgnoreCase("if")) {
            if (ctx.expression().size() != 2 && ctx.expression().size() != 3) {
                throw new ParsingException("Invalid number of arguments for 'if' function", null, ctx.getStart().getLine(), ctx.getStart().getCharPositionInLine());
            }

            Expression elseExpression = null;
            if (ctx.expression().size() == 3) {
                elseExpression = (Expression) visit(ctx.expression(2));
            }

            return new IfExpression(
                    (Expression) visit(ctx.expression(0)),
                    (Expression) visit(ctx.expression(1)),
                    elseExpression);
        }
        else if (name.toString().equalsIgnoreCase("nullif")) {
            if (ctx.expression().size() != 2) {
                throw new ParsingException("Invalid number of arguments for 'nullif' function", null, ctx.getStart().getLine(), ctx.getStart().getCharPositionInLine());
            }
            return new NullIfExpression(
                    (Expression) visit(ctx.expression(0)),
                    (Expression) visit(ctx.expression(1)));
        }
        else if (name.toString().equalsIgnoreCase("coalesce")) {
            return new CoalesceExpression(visitExpressions(ctx.expression()));
        }

        return new FunctionCall(name, null, false, visitExpressions(ctx.expression()));
    }

    @Override
    public Node visitOver(@NotNull SqlParser.OverContext ctx)
    {
        List<SortItem> orderBy = ctx.sortItem().stream()
                .map(this::visit)
                .map(SortItem.class::cast)
                .collect(Collectors.toList());

        WindowFrame frame = Optional.ofNullable(ctx.windowFrame())
                .map(this::visit)
                .map(WindowFrame.class::cast)
                .orElse(null);

        return new Window(visitExpressions(ctx.partition), orderBy, frame);
    }

    @Override
    public Node visitSortItem(@NotNull SqlParser.SortItemContext ctx)
    {
        SortItem.Ordering orderingType = SortItem.Ordering.ASCENDING;

        if (ctx.ordering != null) {
            switch (ctx.ordering.getType()) {
                case SqlLexer.ASC:
                    orderingType = SortItem.Ordering.ASCENDING;
                    break;
                case SqlLexer.DESC:
                    orderingType = SortItem.Ordering.DESCENDING;
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported ordering: " + ctx.ordering.getText());
            }
        }

        SortItem.NullOrdering nullOrderingType = SortItem.NullOrdering.UNDEFINED;
        if (ctx.nullOrdering != null) {
            switch (ctx.nullOrdering.getType()) {
                case SqlLexer.FIRST:
                    nullOrderingType = SortItem.NullOrdering.FIRST;
                    break;
                case SqlLexer.LAST:
                    nullOrderingType = SortItem.NullOrdering.LAST;
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported ordering: " + ctx.nullOrdering.getText());
            }
        }

        return new SortItem((Expression) visit(ctx.expression()), orderingType, nullOrderingType);
    }

    @Override
    public Node visitWindowFrame(@NotNull SqlParser.WindowFrameContext ctx)
    {
        WindowFrame.Type frameType;

        switch (ctx.frameType.getType()) {
            case SqlLexer.RANGE:
                frameType = WindowFrame.Type.RANGE;
                break;
            case SqlLexer.ROWS:
                frameType = WindowFrame.Type.ROWS;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported frame type: " + ctx.frameType.getText());
        }

        FrameBound start = (FrameBound) visit(ctx.start);
        FrameBound end = Optional.ofNullable(ctx.end)
                .map(this::visit)
                .map(FrameBound.class::cast)
                .orElse(null);

        return new WindowFrame(frameType, start, end);
    }

    @Override
    public Node visitUnboundedFrame(@NotNull SqlParser.UnboundedFrameContext ctx)
    {
        switch (ctx.boundType.getType()) {
            case SqlLexer.PRECEDING:
                return new FrameBound(FrameBound.Type.UNBOUNDED_PRECEDING);
            case SqlLexer.FOLLOWING:
                return new FrameBound(FrameBound.Type.UNBOUNDED_FOLLOWING);
        }

        throw new UnsupportedOperationException("Unsupported bound type: " + ctx.boundType.getText());
    }

    @Override
    public Node visitBoundedFrame(@NotNull SqlParser.BoundedFrameContext ctx)
    {
        Expression bound = (Expression) visit(ctx.expression());

        switch (ctx.boundType.getType()) {
            case SqlLexer.PRECEDING:
                return new FrameBound(FrameBound.Type.PRECEDING, bound);
            case SqlLexer.FOLLOWING:
                return new FrameBound(FrameBound.Type.FOLLOWING, bound);
        }

        throw new UnsupportedOperationException("Unsupported bound type: " + ctx.boundType.getText());
    }

    @Override
    public Node visitCurrentRowBound(@NotNull SqlParser.CurrentRowBoundContext ctx)
    {
        return new FrameBound(FrameBound.Type.CURRENT_ROW);
    }

    // ************** literals **************

    @Override
    public Node visitNullLiteral(@NotNull SqlParser.NullLiteralContext ctx)
    {
        return new NullLiteral();
    }

    @Override
    public Node visitStringLiteral(@NotNull SqlParser.StringLiteralContext ctx)
    {
        return new StringLiteral(unquote(ctx.STRING().getText()));
    }

    @Override
    public Node visitTypeConstructor(@NotNull SqlParser.TypeConstructorContext ctx)
    {
        String type = ctx.identifier().getText();
        String value = unquote(ctx.STRING().getText());

        if (type.equalsIgnoreCase("time")) {
            return new TimeLiteral(value);
        }
        else if (type.equalsIgnoreCase("timestamp")) {
            return new TimestampLiteral(value);
        }

        return new GenericLiteral(type, value);
    }

    @Override
    public Node visitIntegerLiteral(@NotNull SqlParser.IntegerLiteralContext ctx)
    {
        return new LongLiteral(ctx.getText());
    }

    @Override
    public Node visitDecimalLiteral(@NotNull SqlParser.DecimalLiteralContext ctx)
    {
        return new DoubleLiteral(ctx.getText());
    }

    @Override
    public Node visitBooleanValue(@NotNull SqlParser.BooleanValueContext ctx)
    {
        return new BooleanLiteral(ctx.getText());
    }

    @Override
    public Node visitInterval(@NotNull SqlParser.IntervalContext ctx)
    {
        IntervalLiteral.Sign sign = IntervalLiteral.Sign.POSITIVE;
        if (ctx.sign != null) {
            switch (ctx.sign.getType()) {
                case SqlLexer.MINUS:
                    sign = IntervalLiteral.Sign.NEGATIVE;
                    break;
                case SqlLexer.PLUS:
                    sign = IntervalLiteral.Sign.POSITIVE;
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported sign: " + ctx.sign.getText());
            }
        }

        IntervalLiteral.IntervalField from = getIntervalField(ctx.from);
        IntervalLiteral.IntervalField to = Optional.ofNullable(ctx.to)
                .map(AstBuilder::getIntervalField)
                .orElse(null);

        return new IntervalLiteral(unquote(ctx.STRING().getText()), sign, from, to);
    }

    // helpers

    @Override
    protected Node defaultResult()
    {
        return null;
    }

    @Override
    protected Node aggregateResult(Node aggregate, Node nextResult)
    {
        if (nextResult == null) {
            throw new UnsupportedOperationException("not yet implemented");
        }

        if (aggregate == null) {
            return nextResult;
        }

        throw new UnsupportedOperationException("not yet implemented");
    }

    private Optional<Expression> visitIfPresent(ParserRuleContext context)
    {
        return Optional.ofNullable(context)
                .map(this::visit)
                .map(Expression.class::cast);
    }

    private List<Expression> visitExpressions(List<? extends ParserRuleContext> context)
    {
        return context.stream()
                .map(this::visit)
                .map(Expression.class::cast)
                .collect(Collectors.toList());
    }

    private static String unquote(String string)
    {
        return string.substring(1, string.length() - 1);
    }

    private static QualifiedName getQualifiedName(SqlParser.QualifiedNameContext context)
    {
        List<String> parts = context
                .identifier().stream()
                .map(ParseTree::getText)
                .collect(Collectors.toList());

        return new QualifiedName(parts);
    }

    private static IntervalLiteral.IntervalField getIntervalField(SqlParser.IntervalFieldContext context)
    {
        IntervalLiteral.IntervalField field;
        Token token = (Token) context.getChild(0).getPayload();
        switch (token.getType()) {
            case SqlLexer.YEAR:
                field = IntervalLiteral.IntervalField.YEAR;
                break;
            case SqlLexer.MONTH:
                field = IntervalLiteral.IntervalField.MONTH;
                break;
            case SqlLexer.DAY:
                field = IntervalLiteral.IntervalField.DAY;
                break;
            case SqlLexer.HOUR:
                field = IntervalLiteral.IntervalField.HOUR;
                break;
            case SqlLexer.MINUTE:
                field = IntervalLiteral.IntervalField.MINUTE;
                break;
            case SqlLexer.SECOND:
                field = IntervalLiteral.IntervalField.SECOND;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported interval field: " + token.getText());
        }
        return field;
    }

    private static boolean isDistinct(SqlParser.SetQuantifierContext setQuantifier)
    {
        return setQuantifier != null && setQuantifier.DISTINCT() != null;
    }

    private static Optional<String> getTextIfPresent(ParserRuleContext context)
    {
        return Optional.ofNullable(context)
                .map(ParseTree::getText);
    }

    private static Optional<String> getTextIfPresent(Token token)
    {
        return Optional.ofNullable(token)
                .map(Token::getText);
    }

    private static List<String> getColumnAliases(SqlParser.ColumnAliasesContext columnAliasesContext)
    {
        List<String> columnNames = null;
        if (columnAliasesContext != null) {
            columnNames = columnAliasesContext
                    .identifier().stream()
                    .map(ParseTree::getText)
                    .collect(Collectors.toList());
        }
        return columnNames;
    }
}
