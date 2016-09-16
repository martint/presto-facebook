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
package com.facebook.presto.sql.optimizer.old.engine;

import com.facebook.presto.sql.optimizer.old.rule.CombineFilterAndCrossJoin;
import com.facebook.presto.sql.optimizer.old.rule.CombineFilters;
import com.facebook.presto.sql.optimizer.old.rule.CombineGlobalLimits;
import com.facebook.presto.sql.optimizer.old.rule.CombineIntersects;
import com.facebook.presto.sql.optimizer.old.rule.CombineLocalLimits;
import com.facebook.presto.sql.optimizer.old.rule.CombineProjections;
import com.facebook.presto.sql.optimizer.old.rule.CombineScanFilterProject;
import com.facebook.presto.sql.optimizer.old.rule.CombineUnions;
import com.facebook.presto.sql.optimizer.old.rule.GetToScan;
import com.facebook.presto.sql.optimizer.old.rule.IntersectToUnion;
import com.facebook.presto.sql.optimizer.old.rule.OrderByLimitToTopN;
import com.facebook.presto.sql.optimizer.old.rule.PushAggregationThroughUnion;
import com.facebook.presto.sql.optimizer.old.rule.PushFilterThroughAggregation;
import com.facebook.presto.sql.optimizer.old.rule.PushFilterThroughProject;
import com.facebook.presto.sql.optimizer.old.rule.PushFilterThroughSort;
import com.facebook.presto.sql.optimizer.old.rule.PushFilterThroughUnion;
import com.facebook.presto.sql.optimizer.old.rule.PushGlobalLimitThroughUnion;
import com.facebook.presto.sql.optimizer.old.rule.PushGlobalTopNThroughUnion;
import com.facebook.presto.sql.optimizer.old.rule.PushLimitThroughProject;
import com.facebook.presto.sql.optimizer.old.rule.PushLocalTopNThroughUnion;
import com.facebook.presto.sql.optimizer.old.rule.RemoveIdentityProjection;
import com.facebook.presto.sql.optimizer.old.tree.Expression;
import com.facebook.presto.sql.optimizer.old.tree.Reference;
import com.google.common.collect.ImmutableList;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

public class CostBasedOptimizer
    implements Optimizer
{
    private final List<Rule> rules;

    public CostBasedOptimizer(List<Rule> rules)
    {
        this.rules = ImmutableList.copyOf(rules);
    }

    public CostBasedOptimizer()
    {
        this(ImmutableList.of(
                new PushFilterThroughProject(),
                new PushFilterThroughAggregation(),
                new PushFilterThroughUnion(),
                new PushFilterThroughSort(),
                new PushGlobalTopNThroughUnion(),
                new CombineScanFilterProject(),
                new CombineFilterAndCrossJoin(),
                new PushAggregationThroughUnion(),
                new CombineProjections(),
                new RemoveIdentityProjection(),
                new CombineFilters(),
                new CombineGlobalLimits(),
                new CombineLocalLimits(),
                new PushGlobalLimitThroughUnion(),
                new PushLocalTopNThroughUnion(),
                new PushLimitThroughProject(),
                new OrderByLimitToTopN(),
                new IntersectToUnion(),
                new CombineUnions(),
                new CombineIntersects(),
                new GetToScan()
        ));
    }

    public Expression optimize(Expression expression)
    {
        Memo memo = new Memo(true);

        String rootClass = memo.insert(expression);
        System.out.println(memo.toGraphviz());

        long previous;
        long version = memo.getVersion();
        do {
            explore(memo, new HashSet<>(), rules, rootClass, new MemoLookup(memo, rootClass));
            previous = version;
            version = memo.getVersion();
        }
        while (previous != version);

        System.out.println(memo.toGraphviz());
        return null;
//        return memo;
    }

    private void explore(Memo memo, Set<String> explored, List<Rule> rules, String group, MemoLookup lookup)
    {
        if (explored.contains(group)) {
            return;
        }
        explored.add(group);

        Queue<Expression> queue = new ArrayDeque<>();
        memo.getExpressions(group).stream()
                .map(VersionedItem::get)
                .forEach(queue::add);

        while (!queue.isEmpty()) {
            Expression expression = queue.poll();

            for (Rule rule : rules) {
                List<Expression> transformed = rule.apply(expression, lookup)
                        .collect(Collectors.toList());

                for (Expression e : transformed) {
                    memo.transform(expression, e, rule.getClass().getSimpleName())
                            .ifPresent(queue::add);
                }
            }
        }

        List<Reference> references = memo.getExpressions(group).stream()
                .map(VersionedItem::get)
                .flatMap(e -> e.getArguments().stream())
                .map(Reference.class::cast)
                .collect(Collectors.toList());

        for (Reference reference : references) {
            explore(memo, explored, rules, reference.getName(), lookup.push(reference.getName()));
        }
    }
}
