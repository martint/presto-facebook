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

import com.facebook.presto.sql.optimizer.rule.CombineFilterAndCrossJoin;
import com.facebook.presto.sql.optimizer.rule.CombineFilters;
import com.facebook.presto.sql.optimizer.rule.CombineGlobalLimits;
import com.facebook.presto.sql.optimizer.rule.CombineProjections;
import com.facebook.presto.sql.optimizer.rule.CombineScanFilterProject;
import com.facebook.presto.sql.optimizer.rule.CombineUnions;
import com.facebook.presto.sql.optimizer.rule.GetToScan;
import com.facebook.presto.sql.optimizer.rule.IntersectToUnion;
import com.facebook.presto.sql.optimizer.rule.OrderByLimitToTopN;
import com.facebook.presto.sql.optimizer.rule.PushAggregationThroughUnion;
import com.facebook.presto.sql.optimizer.rule.PushFilterThroughAggregation;
import com.facebook.presto.sql.optimizer.rule.PushFilterThroughProject;
import com.facebook.presto.sql.optimizer.rule.PushFilterThroughSort;
import com.facebook.presto.sql.optimizer.rule.PushFilterThroughUnion;
import com.facebook.presto.sql.optimizer.rule.PushGlobalLimitThroughUnion;
import com.facebook.presto.sql.optimizer.rule.PushLimitThroughProject;
import com.facebook.presto.sql.optimizer.rule.PushLocalLimitThroughUnion;
import com.facebook.presto.sql.optimizer.rule.RemoveIdentityProjection;
import com.facebook.presto.sql.optimizer.tree.Expression;
import com.facebook.presto.sql.optimizer.tree.Reference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Longs;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

public class GreedyOptimizer
        implements Optimizer
{
    private final List<Set<Rule>> batches;

    public GreedyOptimizer(List<Set<Rule>> batches)
    {
        this.batches = ImmutableList.copyOf(batches);
    }

    public GreedyOptimizer()
    {
        this(ImmutableList.of(
                ImmutableSet.of(
                        new IntersectToUnion()
                ),
                ImmutableSet.of(
                        new RemoveIdentityProjection(),
                        new PushFilterThroughProject(),
                        new PushFilterThroughAggregation(),
                        new PushFilterThroughUnion(),
                        new PushFilterThroughSort(),
                        new PushAggregationThroughUnion(),
                        new CombineFilters(),
                        new CombineGlobalLimits(),
                        new CombineProjections(),
                        new PushGlobalLimitThroughUnion(),
                        new PushLocalLimitThroughUnion(),
                        new PushLimitThroughProject(),
                        new CombineUnions()
                ),
                ImmutableSet.of(
                        new CombineScanFilterProject(),
                        new CombineFilterAndCrossJoin(),
                        new OrderByLimitToTopN(),
                        new GetToScan())));
    }

    public Expression optimize(Expression expression)
    {
        Memo memo = new Memo(true);

        String rootClass = memo.insert(expression);

        MemoLookup lookup = new MemoLookup(
                memo,
                rootClass,
                expressions -> expressions.sorted((e1, e2) -> -Longs.compare(e1.getVersion(), e2.getVersion()))
                        .limit(1));

        for (Set<Rule> batch : batches) {
            long previous;
            long version = memo.getVersion();
            do {
                explore(memo, lookup, new HashSet<>(), batch, rootClass);
                previous = version;
                version = memo.getVersion();
            }
            while (previous != version);
        }

        return extract(rootClass, lookup);
    }

    private Expression extract(String group, MemoLookup lookup)
    {
        Expression expression = lookup.lookup(new Reference(group))
                .findFirst()
                .get();

        List<Expression> children = expression.getArguments().stream()
                .map(Reference.class::cast)
                .map(e -> extract(e.getName(), lookup.push(e.getName())))
                .collect(Collectors.toList());

        return expression.copyWithArguments(children);
    }

    private void explore(Memo memo, MemoLookup lookup, Set<String> explored, Set<Rule> rules, String group)
    {
        if (explored.contains(group)) {
            return;
        }
        explored.add(group);

        Expression expression = lookup.lookup(new Reference(group))
                .findFirst()
                .get();

        expression.getArguments().stream()
                .map(Reference.class::cast)
                .map(Reference::getName)
                .forEach(name -> explore(memo, lookup.push(name), explored, rules, name));

        boolean madeProgress;
        do {
            madeProgress = false;
            for (Rule rule : rules) {
                List<Expression> transformed = rule.apply(expression, lookup)
                        .collect(Collectors.toList());

                checkState(transformed.size() <= 1, "Expected one expression");
                if (!transformed.isEmpty()) {
                    Optional<Expression> rewritten = memo.transform(expression, transformed.get(0), rule.getClass().getSimpleName());
                    if (rewritten.isPresent()) {
                        madeProgress = true;
                        expression = rewritten.get();
                    }
                }
            }
        }
        while (madeProgress);

//        expression.getArguments().stream()
//                .map(Reference.class::cast)
//                .map(Reference::getName)
//                .forEach(name -> explore(memo, explored, rules, name));
    }
}
