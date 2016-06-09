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

import com.facebook.presto.sql.optimizer.engine.Memo2;
import com.facebook.presto.sql.optimizer.engine.Rule;
import com.facebook.presto.sql.optimizer.rule.GetToScan;
import com.facebook.presto.sql.optimizer.rule.IntersectToUnion;
import com.facebook.presto.sql.optimizer.rule.MergeFilterAndCrossJoin;
import com.facebook.presto.sql.optimizer.rule.MergeFilters;
import com.facebook.presto.sql.optimizer.rule.MergeLimits;
import com.facebook.presto.sql.optimizer.rule.OrderByLimitToTopN;
import com.facebook.presto.sql.optimizer.rule.PushAggregationThroughUnion;
import com.facebook.presto.sql.optimizer.rule.PushFilterIntoScan;
import com.facebook.presto.sql.optimizer.rule.PushFilterThroughAggregation;
import com.facebook.presto.sql.optimizer.rule.PushFilterThroughProject;
import com.facebook.presto.sql.optimizer.rule.PushFilterThroughUnion;
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

public class Main5
{
    private Main5()
    {
    }

    public static void main(String[] args)
            throws InterruptedException
    {
        Memo2 memo = new Memo2();

        Expression<?> root =
        new Limit(3,
                new Sort("s0",
                        new Filter("f0",
                                new Aggregate(Aggregate.Type.SINGLE, "a1",
                                        new Limit(5,
                                                new Limit(10,
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

//        Expression<?> root =
//                new Limit(10, new Limit(5, new Get("t")));

        String rootClass = memo.insert(root);

        List<Rule> rules = ImmutableList.of(
                new PushFilterThroughProject(),
                new PushAggregationThroughUnion(),
                new PushFilterThroughAggregation(),
                new PushFilterThroughUnion(),
                new MergeFilters(),
                new PushFilterIntoScan(),
                new MergeLimits(),
                new PushLimitThroughUnion(),
                new OrderByLimitToTopN(),
                new IntersectToUnion(),
                new MergeFilterAndCrossJoin(),
                new GetToScan());

        System.out.println(memo.toGraphviz());

        explore(memo, new HashSet<>(), rules, rootClass);
//        System.out.println(memo.toGraphviz());

        explore(memo, new HashSet<>(), rules, rootClass);
//        System.out.println(memo.toGraphviz());

//        explore(memo, new HashSet<>(), rules, rootClass);
        System.out.println(memo.toGraphviz());
    }

    private static void explore(Memo2 memo, Set<String> explored, List<Rule> rules, String group)
    {
        if (explored.contains(group)) {
            return;
        }
        explored.add(group);

        Queue<Expression<?>> queue = new ArrayDeque<>();

        memo.lookup().lookup(new Reference(group))
                .forEach(queue::add);

        while (!queue.isEmpty()) {
            Expression<?> expression = queue.poll();

            rules.forEach(rule -> {
                rule.apply(expression, memo.lookup())
                        .forEach(transformed -> {
                            Expression<?> rewritten = memo.insert(group, transformed);
                            queue.add(rewritten);
                        });
            });
        }

        List<Reference> references = memo.lookup().lookup(new Reference(group))
                .flatMap(e -> e.getArguments().stream())
                .map(Reference.class::cast)
                .collect(Collectors.toList());

        for (Reference reference : references) {
            explore(memo, explored, rules, reference.getName());
        }
    }
}
