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
package com.facebook.presto.sql.optimizer.engine;

import com.facebook.presto.sql.optimizer.rule.FlattenIntersect;
import com.facebook.presto.sql.optimizer.rule.FlattenUnion;
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
import com.facebook.presto.sql.optimizer.rule.RemoveIdentityProjection;
import com.facebook.presto.sql.optimizer.tree.Expression;
import com.facebook.presto.sql.optimizer.tree.Reference;
import com.google.common.collect.ImmutableList;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

public class Optimizer
{
    private final List<Rule> rules;

    public Optimizer(List<Rule> rules)
    {
        this.rules = ImmutableList.copyOf(rules);
    }

    public Optimizer()
    {
        this(ImmutableList.of(
                new PushFilterThroughProject(),
                new PushFilterThroughAggregation(),
                new PushFilterThroughUnion(),
                new PushFilterIntoScan(),
                new MergeFilterAndCrossJoin(),
                new PushAggregationThroughUnion(),
                new MergeProjections(),
                new RemoveIdentityProjection(),
                new MergeFilters(),
                new MergeLimits(),
                new PushLimitThroughUnion(),
                new PushLimitThroughProject(),
                new OrderByLimitToTopN(),
                new IntersectToUnion(),
                new FlattenUnion(),
                new FlattenIntersect(),
                new GetToScan()
        ));
    }

    public Memo optimize(Expression expression)
    {
        Memo memo = new Memo(true);

        String rootClass = memo.insert(expression);
        System.out.println(memo.toGraphviz());

        long previous;
        long version = memo.getVersion();
        do {
            explore(memo, new HashSet<>(), rules, rootClass);
            previous = version;
            version = memo.getVersion();
        }
        while (previous != version);

        return memo;
    }

    private void explore(Memo memo, Set<String> explored, List<Rule> rules, String group)
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
                Lookup lookup = new MemoLookup(memo, group);

                List<Expression> transformed = rule.apply(expression, lookup)
                        .collect(Collectors.toList());

                for (Expression e : transformed) {
                    memo.transform(expression, e, rule.getClass().getSimpleName())
                            .ifPresent(queue::add);
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
}
