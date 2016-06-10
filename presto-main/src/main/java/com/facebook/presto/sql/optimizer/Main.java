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
package com.facebook.presto.sql.optimizer;

import com.facebook.presto.sql.optimizer.engine.Lookup;
import com.facebook.presto.sql.optimizer.engine.Memo2;
import com.facebook.presto.sql.optimizer.engine.Rule;
import com.facebook.presto.sql.optimizer.rule.GetToScan;
import com.facebook.presto.sql.optimizer.rule.IntersectToUnion;
import com.facebook.presto.sql.optimizer.rule.MergeFilterAndCrossJoin;
import com.facebook.presto.sql.optimizer.rule.MergeFilters;
import com.facebook.presto.sql.optimizer.rule.MergeLimits;
import com.facebook.presto.sql.optimizer.rule.MergeProjections;
import com.facebook.presto.sql.optimizer.rule.OrderByLimitToTopN;
import com.facebook.presto.sql.optimizer.rule.PushAggregationThroughUnion;
import com.facebook.presto.sql.optimizer.rule.PushFilterIntoScan;
import com.facebook.presto.sql.optimizer.rule.PushFilterThroughAggregation;
import com.facebook.presto.sql.optimizer.rule.PushFilterThroughProject;
import com.facebook.presto.sql.optimizer.rule.PushFilterThroughUnion;
import com.facebook.presto.sql.optimizer.rule.PushLimitThroughProject;
import com.facebook.presto.sql.optimizer.rule.PushLimitThroughUnion;
import com.facebook.presto.sql.optimizer.tree.Aggregate;
import com.facebook.presto.sql.optimizer.tree.CrossJoin;
import com.facebook.presto.sql.optimizer.tree.Expression;
import com.facebook.presto.sql.optimizer.tree.Filter;
import com.facebook.presto.sql.optimizer.tree.Get;
import com.facebook.presto.sql.optimizer.tree.Intersect;
import com.facebook.presto.sql.optimizer.tree.Limit;
import com.facebook.presto.sql.optimizer.tree.Project;
import com.facebook.presto.sql.optimizer.tree.Reference;
import com.facebook.presto.sql.optimizer.tree.Sort;
import com.facebook.presto.sql.optimizer.tree.Union;
import com.google.common.collect.ImmutableList;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main
{
    private Main()
    {
    }

    public static void main(String[] args)
            throws InterruptedException
    {
        List<Rule> rules = ImmutableList.of(
                new PushFilterThroughProject(),
                new PushFilterThroughAggregation(),
                new PushFilterThroughUnion(),
                new PushFilterIntoScan(),
                new MergeFilterAndCrossJoin(),
                new PushAggregationThroughUnion(),
                new MergeProjections(),
                new MergeFilters(),
                new MergeLimits(),
                new PushLimitThroughUnion(),
                new PushLimitThroughProject(),
                new OrderByLimitToTopN(),
                new IntersectToUnion(),
                new GetToScan()
        );

        Memo2 memo = new Memo2();

        Expression root =
        new Limit(3,
                new Sort("s0",
                        new Filter("f0",
                                new Aggregate(Aggregate.Type.SINGLE, "a1",
                                        new Limit(10,
                                                new Limit(5,
                                                        new Union(
                                                                new Filter("f1",
                                                                        new Project("p1",
                                                                                new Get("t")
                                                                        )
                                                                ),
                                                                new Filter("f2",
                                                                        new CrossJoin(
                                                                                new Get("u"),
                                                                                new Project("p2",
                                                                                        new Get("t")
                                                                                )
                                                                        )
                                                                ),
                                                                new Intersect(
                                                                        new Get("t"),
                                                                        new Get("u"))
                                                        )
                                                )
                                        )
                                )
                        )
                )
        );

//        Expression root =
        new Limit(10,
                new Limit(5,
                        new Project("p",
                                new Get("t"))));

//        Expression root =
        new Filter("f1",
                new Filter("f2",
                        new Get("t")
                )
        );

//        Expression root =
        new Filter("f",
                new Limit(5,
                        new Project("p",
                                new Get("t"))));

//        Expression root =
                new Union(
                        new Get("t"),
                        new Get("t")
                );

        String rootClass = memo.insert(root);
        System.out.println(memo.toGraphviz());

        long previous;
        long version = memo.getVersion();
        do {
            explore(memo, new HashSet<>(), rules, rootClass);
            previous = version;
            version = memo.getVersion();
        }
        while (previous != version);

//        System.out.println(memo.getVersion());
        System.out.println(memo.toGraphviz());

//        System.out.println(memo.dump());
    }

    private static void explore(Memo2 memo, Set<String> explored, List<Rule> rules, String group)
    {
        if (explored.contains(group)) {
            return;
        }
        explored.add(group);

        Queue<Expression> queue = new ArrayDeque<>();
        queue.addAll(memo.getExpressions(group));

        while (!queue.isEmpty()) {
            Expression expression = queue.poll();

            for (Rule rule : rules) {
                Lookup lookup = new LookupImpl(memo, group);

                List<Expression> transformed = rule.apply(expression, lookup)
                        .collect(Collectors.toList());

                for (Expression e : transformed) {
                    Expression rewritten = memo.insert(group, e);
                    queue.add(rewritten);
                }
            }
        }

        List<Reference> references = memo.getExpressions(group).stream()
                .flatMap(e -> e.getArguments().stream())
                .map(Reference.class::cast)
                .collect(Collectors.toList());

        for (Reference reference : references) {
            explore(memo, explored, rules, reference.getName());
        }
    }

    private static class LookupImpl
            implements Lookup
    {
        private final Set<Expression> visited = new HashSet<>();
        private final Memo2 memo;

        public LookupImpl(Memo2 memo, String group)
        {
            this.memo = memo;
            visited.add(new Reference(group));
        }

        @Override
        public Stream<Expression> lookup(Expression expression)
        {
            if (visited.contains(expression)) {
                return Stream.empty();
            }
            visited.add(expression);

            if (expression instanceof Reference) {
                return memo.getExpressions(((Reference) expression).getName()).stream();
            }
            return Stream.of(expression);
        }
    }
}
