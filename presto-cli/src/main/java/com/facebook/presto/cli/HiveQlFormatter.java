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
package com.facebook.presto.cli;

import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.JoinUsing;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NaturalJoin;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.cli.HiveQlExpressionFormatter.formatExpression;

public class HiveQlFormatter
{

    public static String format(Node node)
    {
        StringBuilder builder = new StringBuilder();
        new Formatter().process(node, new Output(builder));
        return builder.toString();
    }

    private static class Output
    {
        private static final String INDENT = "   ";

        private final StringBuilder builder;
        private final int indent;

        public Output(StringBuilder builder)
        {
            this(builder, 0);
        }

        private Output(StringBuilder builder, int indent)
        {
            this.builder = builder;
            this.indent = indent;
        }

        public Output indent()
        {
            return new Output(builder, indent + 1);
        }

        public Output unindent()
        {
            Preconditions.checkState(indent >= 0, "Already at indent 0");
            return new Output(builder, indent - 1);
        }

        public Output appendIndented(String value)
        {
            builder.append(Strings.repeat(INDENT, indent))
                    .append(value);

            return this;
        }

        public Output append(String value)
        {
            builder.append(value);
            return this;
        }

        public Output append(char value)
        {
            builder.append(value);
            return this;
        }

        public Output append(double value)
        {
            builder.append(value);
            return this;
        }
    }

    private static class Formatter
            extends AstVisitor<Void, Output>
    {
        @Override
        protected Void visitNode(Node node, Output output)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        @Override
        protected Void visitExpression(Expression node, Output output)
        {
            output.append(formatExpression(node));
            return null;
        }

        @Override
        protected Void visitCreateTable(CreateTable node, Output output)
        {
            output.appendIndented("CREATE TABLE ")
                    .append(formatIdentifier(node.getName()))
                    .append(" AS ");

            process(node.getQuery(), output);

            return null;
        }

        @Override
        protected Void visitQuery(com.facebook.presto.sql.tree.Query node, Output output)
        {
            if (node.getWith().isPresent()) {
                throw new IllegalArgumentException("WITH not supported by HiveQL");
            }

            process(node.getQueryBody(), output);

            if (!node.getOrderBy().isEmpty()) {
                output.appendIndented("ORDER BY " + Joiner.on(", ").join(Iterables.transform(node.getOrderBy(), orderByFormatterFunction())))
                        .append('\n');
            }

            if (node.getLimit().isPresent()) {
                output.appendIndented("LIMIT " + node.getLimit().get())
                        .append('\n');
            }

            return null;
        }

        @Override
        protected Void visitQuerySpecification(QuerySpecification node, Output output)
        {
            process(node.getSelect(), output);

            if (node.getFrom() != null) {
                output.appendIndented("FROM");
                if (node.getFrom().size() > 1) {
                    output.append('\n');
                    output.appendIndented("  ");
                    Iterator<Relation> relations = node.getFrom().iterator();
                    while (relations.hasNext()) {
                        process(relations.next(), output);
                        if (relations.hasNext()) {
                            output.append('\n');
                            output.appendIndented(", ");
                        }
                    }
                }
                else {
                    output.append(' ');
                    process(Iterables.getOnlyElement(node.getFrom()), output);
                }
            }

            output.append('\n');

            if (node.getWhere().isPresent()) {
                output.appendIndented("WHERE " + formatExpression(node.getWhere().get()))
                        .append('\n');
            }

            if (!node.getGroupBy().isEmpty()) {
                output.appendIndented("GROUP BY ");

                for (int i = 0; i < node.getGroupBy().size(); i++) {
                    Expression expression = node.getGroupBy().get(i);
                    if (expression instanceof LongLiteral) {
                        int index = (int) ((LongLiteral) expression).getValue() - 1;
                        expression = ((SingleColumn) node.getSelect().getSelectItems().get(index)).getExpression();
                    }

                    output.append(formatExpression(expression));
                    if (i < node.getGroupBy().size() - 1) {
                        output.append(", ");
                    }
                }
                output.append('\n');
            }

            if (node.getHaving().isPresent()) {
                output.appendIndented("HAVING " + formatExpression(node.getHaving().get()))
                        .append('\n');
            }

            if (!node.getOrderBy().isEmpty()) {
                output.appendIndented("ORDER BY ");

                for (int i = 0; i < node.getOrderBy().size(); i++) {
                    SortItem sortItem = node.getOrderBy().get(i);
                    if (sortItem.getSortKey() instanceof LongLiteral) {
                        int index = (int) ((LongLiteral) sortItem.getSortKey()).getValue() - 1;
                        Expression expression = ((SingleColumn) node.getSelect().getSelectItems().get(index)).getExpression();
                        sortItem = new SortItem(expression, sortItem.getOrdering(), sortItem.getNullOrdering());
                    }

                    output.append(orderByFormatterFunction().apply(sortItem));

                    if (i < node.getOrderBy().size() - 1) {
                        output.append(", ");
                    }
                }
                output.append('\n');
            }

            if (node.getLimit().isPresent()) {
                output.appendIndented("LIMIT " + node.getLimit().get())
                        .append('\n');
            }
            return null;
        }

        @Override
        protected Void visitSelect(Select node, Output output)
        {
            output.appendIndented("SELECT");
            if (node.isDistinct()) {
                output.append(" DISTINCT");
            }

            if (node.getSelectItems().size() > 1) {
                boolean first = true;
                for (SelectItem item : node.getSelectItems()) {
                    output.append("\n")
                            .appendIndented(first ? "  " : ", ");

                    process(item, output);
                    first = false;
                }
            }
            else {
                output.append(' ');
                process(Iterables.getOnlyElement(node.getSelectItems()), output);
            }

            output.append('\n');

            return null;
        }

        @Override
        protected Void visitSingleColumn(SingleColumn node, Output output)
        {
            output.append(formatExpression(node.getExpression()));
            if (node.getAlias().isPresent()) {
                output.append(' ')
                        .append('`')
                        .append(node.getAlias().get())
                        .append('`');
            }

            return null;
        }

        @Override
        protected Void visitAllColumns(AllColumns node, Output output)
        {
            output.append(node.toString());

            return null;
        }

        @Override
        protected Void visitTable(Table node, Output output)
        {
            output.append(formatIdentifier(node.getName()));
            return null;
        }

        @Override
        protected Void visitJoin(Join node, Output output)
        {
            JoinCriteria criteria = node.getCriteria();
            String type = node.getType().toString();

            output.append('(');
            process(node.getLeft(), output);

            output.append('\n');
            output.appendIndented(type).append(" JOIN ");

            process(node.getRight(), output);

            if (criteria instanceof JoinUsing) {
                // TODO: translate into JOIN ON
                throw new UnsupportedOperationException("JOIN USING not supported");
            }
            else if (criteria instanceof JoinOn) {
                JoinOn on = (JoinOn) criteria;
                output.append(" ON (")
                        .append(formatExpression(on.getExpression()))
                        .append(")");
            }
            else if (criteria instanceof NaturalJoin) {
                throw new UnsupportedOperationException("NATURAL join not supported");
            }
            else {
                throw new UnsupportedOperationException("unknown join criteria: " + criteria);
            }

            output.append(")");

            return null;
        }

        @Override
        protected Void visitAliasedRelation(AliasedRelation node, Output output)
        {
            process(node.getRelation(), output);

            output.append(' ')
                    .append(node.getAlias());

            if (node.getColumnNames() != null) {
                throw new UnsupportedOperationException("Column aliases not supported");
            }

            return null;
        }

        @Override
        protected Void visitSampledRelation(SampledRelation node, Output output)
        {
            if (node.getType() == SampledRelation.Type.BERNOULLI) {
                throw new UnsupportedOperationException("TABLESAMPLE BERNOULLI not supported");
            }

            process(node.getRelation(), output);

            double percentage;
            if (node.getSamplePercentage() instanceof DoubleLiteral) {
                percentage = ((DoubleLiteral) node.getSamplePercentage()).getValue();
            }
            else if (node.getSamplePercentage() instanceof LongLiteral) {
                percentage = ((LongLiteral) node.getSamplePercentage()).getValue();
            }
            else {
                throw new UnsupportedOperationException("non-numeric sampling ratio not supported");
            }

            output.append(" TABLESAMPLE ")
                    .append(" (")
                    .append(percentage)
                    .append(" PERCENT)");

            return null;
        }

        @Override
        protected Void visitTableSubquery(TableSubquery node, Output output)
        {
            output.append('(')
                    .append('\n');

            process(node.getQuery(), output.indent());

            output.appendIndented(")");

            return null;
        }
    }

    private static String formatIdentifier(QualifiedName name)
    {
        List<String> parts = Lists.transform(name.getParts(), new Function<String, String>()
        {
            @Override
            public String apply(String input)
            {
                return input.replace('@', ':');
            }
        });

        return Joiner.on('.').join(parts);
    }


    public static Function<SortItem, String> orderByFormatterFunction()
    {
        return new Function<SortItem, String>()
        {
            @Override
            public String apply(SortItem input)
            {
                StringBuilder builder = new StringBuilder();

                builder.append(formatExpression(input.getSortKey()));

                switch (input.getOrdering()) {
                    case ASCENDING:
                        builder.append(" ASC");
                        break;
                    case DESCENDING:
                        builder.append(" DESC");
                        break;
                    default:
                        throw new UnsupportedOperationException("unknown ordering: " + input.getOrdering());
                }

                switch (input.getNullOrdering()) {
                    case FIRST:
                    case LAST:
                        throw new UnsupportedOperationException("not yet implemented: non-default null ordering");
                    case UNDEFINED:
                        // no op
                        break;
                    default:
                        throw new UnsupportedOperationException("unknown null ordering: " + input.getNullOrdering());
                }

                return builder.toString();
            }
        };
    }

    public static String formatIdentifier(String s)
    {
        // TODO: handle escaping properly
        return '`' + s + '`';
    }

}
