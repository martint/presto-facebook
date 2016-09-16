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
package com.facebook.presto.sql.optimizer.old.tree;

import com.google.common.collect.ImmutableList;

import java.io.StringWriter;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.optimizer.utils.CollectionConstructors.list;

public class Formatter
{
    private Formatter()
    {
    }

    public static String format(Expression expression)
    {
        StringWriter out = new StringWriter();
//        ListFormatter.format(new PrintWriter(out, true), toList(expression), 0);
        return out.toString();
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

    }
}
