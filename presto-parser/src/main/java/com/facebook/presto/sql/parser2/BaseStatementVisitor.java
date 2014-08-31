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

import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.AbstractParseTreeVisitor;

public class BaseStatementVisitor<T>
    extends AbstractParseTreeVisitor<T>
    implements StatementVisitor<T>
{

    @Override
    public T visitExpression(@NotNull StatementParser.ExpressionContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitSelectItem(@NotNull StatementParser.SelectItemContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitWhereClause(@NotNull StatementParser.WhereClauseContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitColumnConstDef(@NotNull StatementParser.ColumnConstDefContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitShowSchemas(@NotNull StatementParser.ShowSchemasContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitShowTablesFrom(@NotNull StatementParser.ShowTablesFromContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitLimitClause(@NotNull StatementParser.LimitClauseContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitCreateView(@NotNull StatementParser.CreateViewContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitInList(@NotNull StatementParser.InListContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitShowPartitions(@NotNull StatementParser.ShowPartitionsContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitAlterTable(@NotNull StatementParser.AlterTableContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitShowColumns(@NotNull StatementParser.ShowColumnsContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitType(@NotNull StatementParser.TypeContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitExplainOption(@NotNull StatementParser.ExplainOptionContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitSetQuant(@NotNull StatementParser.SetQuantContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitFunctionCall(@NotNull StatementParser.FunctionCallContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitWhenClause(@NotNull StatementParser.WhenClauseContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitOver(@NotNull StatementParser.OverContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitTableFactor(@NotNull StatementParser.TableFactorContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitOrdering(@NotNull StatementParser.OrderingContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitExplainOptions(@NotNull StatementParser.ExplainOptionsContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitJoinCriteria(@NotNull StatementParser.JoinCriteriaContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitIntervalSign(@NotNull StatementParser.IntervalSignContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitShowTablesLike(@NotNull StatementParser.ShowTablesLikeContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitColumnConst(@NotNull StatementParser.ColumnConstContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitDropView(@NotNull StatementParser.DropViewContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitRelation(@NotNull StatementParser.RelationContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitAliasedColumns(@NotNull StatementParser.AliasedColumnsContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitNumber(@NotNull StatementParser.NumberContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitCharlen(@NotNull StatementParser.CharlenContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitExactNumType(@NotNull StatementParser.ExactNumTypeContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitStatement(@NotNull StatementParser.StatementContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitSingleExpression(@NotNull StatementParser.SingleExpressionContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitDropTable(@NotNull StatementParser.DropTableContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitIntervalField(@NotNull StatementParser.IntervalFieldContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitWindow(@NotNull StatementParser.WindowContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitQueryBody(@NotNull StatementParser.QueryBodyContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitTableElement(@NotNull StatementParser.TableElementContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitShowTable(@NotNull StatementParser.ShowTableContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitExplain(@NotNull StatementParser.ExplainContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitComparisonExpression(@NotNull StatementParser.ComparisonExpressionContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitNumlen(@NotNull StatementParser.NumlenContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitTableContentsSource(@NotNull StatementParser.TableContentsSourceContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitWithQuery(@NotNull StatementParser.WithQueryContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitCreateTable(@NotNull StatementParser.CreateTableContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitSimpleQuery(@NotNull StatementParser.SimpleQueryContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitQuery(@NotNull StatementParser.QueryContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitOrReplace(@NotNull StatementParser.OrReplaceContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitElseClause(@NotNull StatementParser.ElseClauseContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitCmpOp(@NotNull StatementParser.CmpOpContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitIntervalLiteral(@NotNull StatementParser.IntervalLiteralContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitIdent(@NotNull StatementParser.IdentContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitTableElementList(@NotNull StatementParser.TableElementListContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitOrderClause(@NotNull StatementParser.OrderClauseContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitCaseExpression(@NotNull StatementParser.CaseExpressionContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitCharType(@NotNull StatementParser.CharTypeContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitJoinedTable(@NotNull StatementParser.JoinedTableContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitSingleStatement(@NotNull StatementParser.SingleStatementContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitExpressionTerm(@NotNull StatementParser.ExpressionTermContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitShowCatalogs(@NotNull StatementParser.ShowCatalogsContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitStratifyOn(@NotNull StatementParser.StratifyOnContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitInteger(@NotNull StatementParser.IntegerContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitFrameBound(@NotNull StatementParser.FrameBoundContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitSpecialFunction(@NotNull StatementParser.SpecialFunctionContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitQualifiedName(@NotNull StatementParser.QualifiedNameContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitShowSchemasFrom(@NotNull StatementParser.ShowSchemasFromContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitTablePrimary(@NotNull StatementParser.TablePrimaryContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitTableRef(@NotNull StatementParser.TableRefContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitDateType(@NotNull StatementParser.DateTypeContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitWindowPartition(@NotNull StatementParser.WindowPartitionContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitWindowFrame(@NotNull StatementParser.WindowFrameContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitDataType(@NotNull StatementParser.DataTypeContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitUseCollection(@NotNull StatementParser.UseCollectionContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitNonReserved(@NotNull StatementParser.NonReservedContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitBooleanExpression(@NotNull StatementParser.BooleanExpressionContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitNullOrdering(@NotNull StatementParser.NullOrderingContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitBool(@NotNull StatementParser.BoolContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitJoinType(@NotNull StatementParser.JoinTypeContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitInsert(@NotNull StatementParser.InsertContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitShowFunctions(@NotNull StatementParser.ShowFunctionsContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitSampleType(@NotNull StatementParser.SampleTypeContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitRowValue(@NotNull StatementParser.RowValueContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitSortItem(@NotNull StatementParser.SortItemContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public T visitLiteral(@NotNull StatementParser.LiteralContext ctx)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
