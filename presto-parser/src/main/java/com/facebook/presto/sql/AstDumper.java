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
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Values;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import java.util.List;

import static com.facebook.presto.sql.ExpressionFormatter.formatExpression;
import static com.google.common.base.Preconditions.checkArgument;

public final class AstDumper
{
    private static final String INDENT = "   ";

    private AstDumper() {}

    public static String dump(Node root)
    {
        StringBuilder builder = new StringBuilder();
        new Formatter(builder).process(root, 0);
        return builder.toString();
    }

    private static class Formatter
            extends AstVisitor<Void, Integer>
    {
        private final StringBuilder builder;

        public Formatter(StringBuilder builder)
        {
            this.builder = builder;
        }

        @Override
        protected Void visitNode(Node node, Integer indent)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        @Override
        protected Void visitExpression(Expression node, Integer indent)
        {
            builder.append(formatExpression(node));
            return null;
        }

        @Override
        protected Void visitQuery(Query node, Integer indent)
        {
            begin(node, indent);
            if (node.getWith().isPresent()) {
                process(node.getWith().get(), indent + 1);
            }

            process(node.getQueryBody(), indent + 1);

            if (!node.getOrderBy().isEmpty()) {
                process(node.getOrderBy(), indent + 1);
            }

            end(indent);
            return null;
        }

        private void process(List<SortItem> orderBy, int indent)
        {
            begin("order-by", indent);
            for (SortItem item : orderBy) {
                process(item, indent + 1);
            }
            end(indent);
        }

        @Override
        protected Void visitQuerySpecification(QuerySpecification node, Integer indent)
        {
            begin(node, indent);
            process(node.getSelect(), indent + 1);

            if (node.getFrom().isPresent()) {
                begin("from", indent + 1);
                process(node.getFrom().get(), indent + 2);
                end(indent + 1);
            }

            if (node.getWhere().isPresent()) {
                process(node.getWhere().get(), indent + 1);
            }

            if (!node.getGroupBy().isEmpty()) {
                begin("group-by", indent + 1);
                for (Expression expression : node.getGroupBy()) {
                    process(expression, indent + 2);
                }
                end(indent + 1);
            }

            if (node.getHaving().isPresent())
            {
                begin("having", indent + 1);
                process(node.getHaving().get(), indent + 2);
                end(indent + 1);
            }

            if (!node.getOrderBy().isEmpty()) {
                process(node.getOrderBy(), indent + 1);
            }

            if (node.getLimit().isPresent()) {
                throw new UnsupportedOperationException("not yet implemented");
            }

            end(indent);
            return null;
        }

        @Override
        protected Void visitSelect(Select node, Integer indent)
        {
            begin(node, indent);
            for (SelectItem item : node.getSelectItems()) {
                process(item, indent + 1);
            }
            end(indent);

            return null;
        }

        @Override
        protected Void visitSingleColumn(SingleColumn node, Integer indent)
        {
            begin(node, indent);
            append(indent + 1, formatExpression(node.getExpression()) + "\n");
            // TODO: alias
            end(indent);

            return null;
        }

        @Override
        protected Void visitAllColumns(AllColumns node, Integer indent)
        {
            begin(node, indent);
            end(indent);

            return null;
        }

        @Override
        protected Void visitTable(Table node, Integer indent)
        {
            begin(node, indent);
            append(indent + 1, node.getName().toString() + "\n");
            end(indent);
            return null;
        }

        @Override
        protected Void visitJoin(Join node, Integer indent)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        protected Void visitAliasedRelation(AliasedRelation node, Integer indent)
        {
            begin(node, indent);
            append(indent + 1, node.getAlias());
            process(node.getRelation(), indent + 1);
            end(indent);

            return null;
        }

        @Override
        protected Void visitSampledRelation(SampledRelation node, Integer indent)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        protected Void visitValues(Values node, Integer indent)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        protected Void visitRow(Row node, Integer indent)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        protected Void visitTableSubquery(TableSubquery node, Integer indent)
        {
            begin(node, indent);
            process(node.getQuery(), indent + 1);
            end(indent);

            return null;
        }

        @Override
        protected Void visitUnion(Union node, Integer indent)
        {
            begin(node, indent);
            for (Relation relation : node.getRelations()) {
                process(relation, indent + 1);
            }
            end(indent);

            return null;
        }

        @Override
        protected Void visitExcept(Except node, Integer indent)
        {
            begin(node, indent);
            process(node.getLeft(), indent + 1);
            process(node.getRight(), indent + 1);
            end(indent);

            return null;
        }

        @Override
        protected Void visitIntersect(Intersect node, Integer indent)
        {
            begin(node, indent);
            for (Relation relation : node.getRelations()) {
                process(relation, indent + 1);
            }
            end(indent);

            return null;
        }

        @Override
        protected Void visitSortItem(SortItem node, Integer indent)
        {
            begin(node, indent);
            append(indent + 1, formatExpression(node.getSortKey()) + "\n");
            append(indent + 1, node.getOrdering().toString() + "\n");
            append(indent + 1, node.getNullOrdering().toString() + "\n");
            end(indent);
            return null;
        }

        private StringBuilder append(int indent, String value)
        {
            return builder.append(indentString(indent))
                    .append(value);
        }

        private void begin(Node node, Integer indent)
        {
            begin(node.getClass().getSimpleName(), indent);
        }

        private void begin(String name, Integer indent)
        {
            append(indent, name + "\n");
        }

        private void end(int indent)
        {
            //append(indent, ")");
//            builder.append(")");
        }

        private static String indentString(int indent)
        {
            return Strings.repeat(INDENT, indent);
        }
    }

    private static void appendAliasColumns(StringBuilder builder, List<String> columns)
    {
        if ((columns != null) && (!columns.isEmpty())) {
            builder.append(" (");
            Joiner.on(", ").appendTo(builder, columns);
            builder.append(')');
        }
    }
}
