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
package com.facebook.presto.sql;

import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Use;
import com.facebook.presto.sql.tree.Values;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.WindowFrame;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import java.io.PrintStream;
import java.util.IdentityHashMap;

public class TreePrinter
{
    private static final String INDENT = "   ";

    private final IdentityHashMap<QualifiedNameReference, QualifiedName> resolvedNameReferences;
    private final PrintStream out;

    public TreePrinter(IdentityHashMap<QualifiedNameReference, QualifiedName> resolvedNameReferences, PrintStream out)
    {
        this.resolvedNameReferences = new IdentityHashMap<>(resolvedNameReferences);
        this.out = out;
    }

    public void print(Node root)
    {
        AstVisitor<Void, Integer> printer = new DefaultTraversalVisitor<Void, Integer>()
        {
            @Override
            protected Void visitNode(Node node, Integer indentLevel)
            {
                throw new UnsupportedOperationException("not yet implemented: " + node);
            }

            @Override
            protected Void visitQuery(Query node, Integer indentLevel)
            {
                print(indentLevel, "Query");

                if (node.getWith().isPresent()) {
                    process(node.getWith().get(), indentLevel + 1);
                }

                print(indentLevel + 1, "QueryBody");
                process(node.getQueryBody(), indentLevel + 2);

                if (!node.getOrderBy().isEmpty()) {
                    print(indentLevel + 1, "OrderBy");
                    for (SortItem sortItem : node.getOrderBy()) {
                        process(sortItem, indentLevel + 2);
                    }
                }

                if (node.getLimit().isPresent()) {
                    print(indentLevel + 1, "Limit: " + node.getLimit().get());
                }

                return null;
            }

            @Override
            protected Void visitQuerySpecification(QuerySpecification node, Integer indentLevel)
            {
                print(indentLevel, "QuerySpecification ");

                indentLevel++;

                process(node.getSelect(), indentLevel);

                if (node.getFrom().isPresent()) {
                    print(indentLevel, "From");
                    process(node.getFrom().get(), indentLevel + 1);
                }

                if (node.getWhere().isPresent()) {
                    print(indentLevel, "Where");
                    process(node.getWhere().get(), indentLevel + 1);
                }

                if (!node.getGroupBy().isEmpty()) {
                    print(indentLevel, "GroupBy");
                    for (Expression expression : node.getGroupBy()) {
                        process(expression, indentLevel + 1);
                    }
                }

                if (node.getHaving().isPresent()) {
                    print(indentLevel, "Having");
                    process(node.getHaving().get(), indentLevel + 1);
                }

                if (!node.getOrderBy().isEmpty()) {
                    print(indentLevel, "OrderBy");
                    for (SortItem sortItem : node.getOrderBy()) {
                        process(sortItem, indentLevel + 1);
                    }
                }

                if (node.getLimit().isPresent()) {
                    print(indentLevel, "Limit: " + node.getLimit().get());
                }

                return null;
            }

            @Override
            protected Void visitSelect(Select node, Integer indentLevel)
            {
                String distinct = "";
                if (node.isDistinct()) {
                    distinct = "[DISTINCT]";
                }
                print(indentLevel, "Select" + distinct);

                super.visitSelect(node, indentLevel + 1); // visit children

                return null;
            }

            @Override
            protected Void visitAllColumns(AllColumns node, Integer indent)
            {
                if (node.getPrefix().isPresent()) {
                    print(indent, node.getPrefix() + ".*");
                }
                else {
                    print(indent, "*");
                }

                return null;
            }

            @Override
            protected Void visitSingleColumn(SingleColumn node, Integer indent)
            {
                if (node.getAlias().isPresent()) {
                    print(indent, "Alias: " + node.getAlias().get());
                }

                super.visitSingleColumn(node, indent + 1); // visit children

                return null;
            }

            @Override
            protected Void visitComparisonExpression(ComparisonExpression node, Integer indentLevel)
            {
                print(indentLevel, node.getType().toString());

                super.visitComparisonExpression(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitArithmeticExpression(ArithmeticExpression node, Integer indentLevel)
            {
                print(indentLevel, node.getType().toString());

                super.visitArithmeticExpression(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitLogicalBinaryExpression(LogicalBinaryExpression node, Integer indentLevel)
            {
                print(indentLevel, node.getType().toString());

                super.visitLogicalBinaryExpression(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitGenericLiteral(GenericLiteral node, Integer indentLevel)
            {
                print(indentLevel, "Literal[" + node.getType() + ":" + node.getValue() + "]");
                return null;
            }

            @Override
            protected Void visitStringLiteral(StringLiteral node, Integer indentLevel)
            {
                print(indentLevel, "String[" + node.getValue() + "]");
                return null;
            }

            @Override
            protected Void visitBooleanLiteral(BooleanLiteral node, Integer indentLevel)
            {
                print(indentLevel, "Boolean[" + node.getValue() + "]");
                return null;
            }

            @Override
            protected Void visitLongLiteral(LongLiteral node, Integer indentLevel)
            {
                print(indentLevel, "Long[" + node.getValue() + "]");
                return null;
            }

            @Override
            protected Void visitLikePredicate(LikePredicate node, Integer indentLevel)
            {
                print(indentLevel, "LIKE");

                super.visitLikePredicate(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitQualifiedNameReference(QualifiedNameReference node, Integer indentLevel)
            {
                QualifiedName resolved = resolvedNameReferences.get(node);
                String resolvedName = "";
                if (resolved != null) {
                    resolvedName = "=>" + resolved.toString();
                }
                print(indentLevel, "QualifiedName[" + node.getName() + resolvedName + "]");
                return null;
            }

            @Override
            protected Void visitFunctionCall(FunctionCall node, Integer indentLevel)
            {
                String name = Joiner.on('.').join(node.getName().getParts());
                print(indentLevel, "FunctionCall[" + name + "]");
                if (node.isDistinct()) {
                    print(indentLevel + 1, "DISTINCT");
                }

                super.visitFunctionCall(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitTable(Table node, Integer indentLevel)
            {
                String name = Joiner.on('.').join(node.getName().getParts());
                print(indentLevel, "Table[" + name + "]");

                return null;
            }

            @Override
            protected Void visitValues(Values node, Integer indentLevel)
            {
                print(indentLevel, "Values");

                super.visitValues(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitRow(Row node, Integer indentLevel)
            {
                print(indentLevel, "Row");

                super.visitRow(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitAliasedRelation(AliasedRelation node, Integer indentLevel)
            {
                print(indentLevel, "Alias[" + node.getAlias() + "]");

                super.visitAliasedRelation(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitSampledRelation(SampledRelation node, Integer indentLevel)
            {
                String stratifyOn = "";
                if (node.getColumnsToStratifyOn().isPresent()) {
                    stratifyOn = " STRATIFY ON (" + node.getColumnsToStratifyOn().get().toString() + ")";
                }

                print(indentLevel, "TABLESAMPLE[" + node.getType() + " (" + node.getSamplePercentage() + ")" + stratifyOn + "]");

                super.visitSampledRelation(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitTableSubquery(TableSubquery node, Integer indentLevel)
            {
                print(indentLevel, "SubQuery");

                super.visitTableSubquery(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitInPredicate(InPredicate node, Integer indentLevel)
            {
                print(indentLevel, "IN");

                super.visitInPredicate(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitSubqueryExpression(SubqueryExpression node, Integer indentLevel)
            {
                print(indentLevel, "SubQuery");

                super.visitSubqueryExpression(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitSubscriptExpression(SubscriptExpression node, Integer indentLevel)
            {
                print(indentLevel, "[]");

                super.visitSubscriptExpression(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitArrayConstructor(ArrayConstructor node, Integer indentLevel)
            {
                print(indentLevel, "Array");

                super.visitArrayConstructor(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitCast(Cast node, Integer indent)
            {
                if (node.isSafe()) {
                    print(indent, "TRY_CAST[" + node.getType() + "]");
                }
                else {
                    print(indent, "CAST[" + node.getType() + "]");
                }

                super.visitCast(node, indent + 1);

                return null;
            }

            @Override
            protected Void visitSimpleCaseExpression(SimpleCaseExpression node, Integer context)
            {
                print(context, "CASE");

                super.visitSimpleCaseExpression(node, context + 1);

                return null;
            }

            @Override
            protected Void visitSearchedCaseExpression(SearchedCaseExpression node, Integer context)
            {
                print(context, "CASE");

                super.visitSearchedCaseExpression(node, context + 1);

                return null;
            }

            @Override
            protected Void visitWhenClause(WhenClause node, Integer context)
            {
                print(context, "WHEN");

                super.visitWhenClause(node, context + 1);

                return null;
            }

            @Override
            public Void visitWindow(Window node, Integer context)
            {
                print(context, "OVER");

                print(context + 1, "PARTITION BY");
                for (Expression expression : node.getPartitionBy()) {
                    process(expression, context + 2);
                }

                print(context + 1, "ORDER BY");
                for (SortItem item : node.getOrderBy()) {
                    process(item, context + 2);
                }

                node.getFrame().ifPresent((frame) -> process(frame, context + 1));

                return null;
            }

            @Override
            public Void visitWindowFrame(WindowFrame node, Integer context)
            {
                print(context, "FRAME[" + node.getType() + "]");

                super.visitWindowFrame(node, context + 1);

                return null;
            }

            @Override
            public Void visitFrameBound(FrameBound node, Integer context)
            {
                print(context, node.getType().toString());
                super.visitFrameBound(node, context + 1);

                return null;
            }

            @Override
            protected Void visitUse(Use node, Integer context)
            {
                if (node.getCatalog().isPresent()) {
                    print(context, "USE[" + node.getCatalog().get() + "." + node.getSchema() + "]");
                }
                else {
                    print(context, "USE[" + node.getSchema() + "]");
                }

                return null;
            }

            @Override
            protected Void visitQueryBody(QueryBody node, Integer context)
            {
                print(context, "QUERY_BODY");

                super.visitQueryBody(node, context + 1);

                return null;
            }

            @Override
            protected Void visitUnion(Union node, Integer context)
            {
                print(context, "UNION");

                super.visitUnion(node, context + 1);

                return null;
            }

            @Override
            protected Void visitIntersect(Intersect node, Integer context)
            {
                print(context, "INTERSECT");

                super.visitIntersect(node, context + 1);

                return null;
            }

            @Override
            protected Void visitExcept(Except node, Integer context)
            {
                print(context, "EXCEPT");

                super.visitExcept(node, context + 1);

                return null;
            }

            @Override
            protected Void visitWith(With node, Integer context)
            {
                print(context, "With");

                super.visitWith(node, context + 1);

                return null;
            }

            @Override
            protected Void visitWithQuery(WithQuery node, Integer context)
            {
                String aliases = node.getColumnNames() == null? "" : "(" + Joiner.on(",").join(node.getColumnNames()) + ")";

                print(context, "NamedQuery[" + node.getName() + aliases + "]");

                super.visitWithQuery(node, context + 1);

                return null;
            }
        };

        printer.process(root, 0);
    }

    private void print(Integer indentLevel, String value)
    {
        out.println(Strings.repeat(INDENT, indentLevel) + value);
    }
}
