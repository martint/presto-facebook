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
package com.facebook.presto.sql.optimizer.tree;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.optimizer.engine.CollectionConstructors.list;

public class Formatter
{
    private Formatter()
    {
    }

    public static String format(Expression expression)
    {
        StringWriter out = new StringWriter();
        format(new PrintWriter(out, true), toList(expression), 0);
        return out.toString();
    }

    private static String format(Object list)
    {
        StringWriter out = new StringWriter();
        format(new PrintWriter(out, true), list, 0);
        return out.toString();
    }

    private static int format(PrintWriter out, Object item, int indent)
    {
        if (item instanceof List) {
            return format(out, (List<Object>) item, indent);
        }
        else {
            String value = item.toString();
            out.print(item);
            return value.length();
        }
    }

    private static int format(PrintWriter out, List<Object> list, int continuationIndent)
    {
        out.print("(");
        continuationIndent++;

        if (!list.isEmpty()) {
            int nextIndent = format(out, list.get(0), continuationIndent);

            boolean chop = depth(list.get(0)) > 1;
            if (list.size() > 1) {
                continuationIndent = addSpacing(out, continuationIndent, nextIndent, chop);
                format(out, list.get(1), continuationIndent);
            }

            chop = chop || list.size() > 2 && list.subList(1, list.size()).stream().anyMatch(i -> depth(i) > 1);

            for (int i = 2; i < list.size(); i++) {
                continuationIndent = addSpacing(out, continuationIndent, nextIndent, chop);
                format(out, list.get(i), continuationIndent);
            }
        }
        out.print(")");

        return continuationIndent;
    }

    private static int addSpacing(PrintWriter out, int continuationIndent, int nextIndent, boolean chop)
    {
        if (chop) {
            out.println();
            out.print(indent(continuationIndent));
        }
        else {
            out.print(" ");
            continuationIndent++;
            continuationIndent += nextIndent;
        }
        return continuationIndent;
    }

    private static int depth(Object item)
    {
        if (item instanceof List) {
            return 1 + ((List<?>) item).stream()
                    .map(Formatter::depth)
                    .max(Integer::compare)
                    .get();
        }

        return 0;
    }

    private static Object toList(Expression expression)
    {
        if (expression instanceof Let) {
            Let let = (Let) expression;

            ImmutableList.Builder<Object> builder = ImmutableList.builder();
            for (Assignment assignment : let.getAssignments()) {
                builder.add(list(assignment.getVariable(), toList(assignment.getExpression())));
            }

            return list(
                    expression.getName(),
                    builder.build(),
                    toList(let.getExpression()));
        }

        if (expression instanceof Aggregate) {
            return list(expression.getName() + "-" + ((Aggregate) expression).getType().toString().toLowerCase(),
                    ((Aggregate) expression).getFunction(),
                    toList(expression.getArguments().get(0)));
        }

        if (expression instanceof Union || expression instanceof Intersect) {
            ImmutableList.Builder<Object> builder = ImmutableList.builder();
            builder.add(expression.getName());
            builder.addAll(expression.getArguments().stream()
                    .map(Formatter::toList)
                    .collect(Collectors.toList()));
            return builder.build();
        }

        if (expression instanceof Project) {
            return list(expression.getName(), ((Project) expression).getExpression(), toList(expression.getArguments().get(0)));
        }

        if (expression instanceof Get) {
            return list(expression.getName(), "'" + ((Get) expression).getTable() + "'");
        }

        if (expression instanceof Sort) {
            return list(expression.getName(), ((Sort) expression).getCriteria(), toList(expression.getArguments().get(0)));
        }

        if (expression instanceof TopN) {
            TopN topN = (TopN) expression;
            return list(expression.getName(), topN.getCount(), topN.getCriteria(), toList(expression.getArguments().get(0)));
        }

        if (expression instanceof Join) {
            Join join = (Join) expression;
            return list(join.getType().toString().toLowerCase() + "-" + expression.getName(),
                    join.getCriteria(),
                    toList(join.getArguments().get(0)),
                    toList(join.getArguments().get(1)));
        }

        if (expression instanceof GlobalLimit) {
            return list(expression.getName(), ((GlobalLimit) expression).getCount(), toList(expression.getArguments().get(0)));
        }

        if (expression instanceof LocalLimit) {
            return list(expression.getName(), ((LocalLimit) expression).getCount(), toList(expression.getArguments().get(0)));
        }

        if (expression instanceof Scan) {
            return list(expression.getName(), "'" + ((Scan) expression).getTable() + "'");
        }

        if (expression instanceof ScanFilterProject) {
            ScanFilterProject scan = (ScanFilterProject) expression;

            return list(
                    expression.getName(),
                    scan.getFilter(),
                    scan.getProjection(),
                    "'" + scan.getTable() + "'");
        }

        if (expression instanceof Filter) {
            return list(expression.getName(), ((Filter) expression).getCriteria(), toList(expression.getArguments().get(0)));
        }

        if (expression instanceof Lambda) {
            Lambda lambda = (Lambda) expression;
            return list("\\" + lambda.getVariable(), toList(lambda.getExpression()));
        }

        if (expression instanceof Call) {
            ImmutableList.Builder<Object> builder = ImmutableList.builder();
            builder.add(expression.getName());
            builder.add("'" + ((Call) expression).getFunction() + "'");
            for (Expression argument : expression.getArguments()) {
                builder.add(toList(argument));
            }
            return builder.build();
        }

        ImmutableList.Builder<Object> builder = ImmutableList.builder();
        builder.add(expression.getName());
        for (Expression argument : expression.getArguments()) {
            builder.add(toList(argument));
        }

        return builder.build();
    }

    private static String indent(int indent)
    {
        return Strings.repeat(" ", indent);
    }

    public static void main(String[] args)
    {
        Expression expression =
                new Let(
                        list(
                                new Assignment("a", new Get("t")),
                                new Assignment("b", new Get("u")),
                                new Assignment("c", new Join(Join.Type.INNER, "j", new Reference("a"), new Reference("b")))),
                        new Reference("c"));

        expression = new Let(
                list(
                        new Assignment("d", expression),
                        new Assignment("e", new Get("w"))),
                new Reference("e"));

        System.out.println(Formatter.format(expression));
        System.out.println();

        System.out.println(Formatter.format(
                new GlobalLimit(10,
                        new Join(Join.Type.FULL, "j1",
                                new Join(Join.Type.INNER, "j2",
                                        new Get("t"),
                                        new Get("u")),
                                new LocalLimit(10,
                                        new CrossJoin(
                                                new Get("v"),
                                                new EnforceScalar(
                                                        new Get("w"))))))));

        List<Object> list = list(1, 2, 3, 47,
                list(
                        list(list(4, 5), list(6, 7)),
                        list(list(4, 5), list(6, 7))),
                9);

        System.out.println(format(list));
        System.out.println();

        System.out.println(format(list(1, 2, 3, 4)));
        System.out.println();

        System.out.println(format(list(list(1, 2), list(3, 4), list(5, 6))));
        System.out.println();

        System.out.println(format(list(list(list(1, 2), list(3, 4)), 5, 6)));
        System.out.println();

        System.out.println(format(list(1, 2, 3, 4, 5, list(6, 7), list(8, 9))));
        System.out.println();

        System.out.println(format(list(1, 2, 3, 4, 5, list(list(6, 7), list(8, 9)))));
        System.out.println();

        System.out.println(format(list()));
        System.out.println();

        System.out.println(format(list(1)));
        System.out.println();

        System.out.println(format(list(1, 2)));
        System.out.println();

        System.out.println(format(list(1, 2, 3)));
        System.out.println();

        System.out.println(format(list(1, list(list(2, list(3, 7)), list(4, 5)), 6)));
    }
}
