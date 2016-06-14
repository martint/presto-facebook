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
import com.facebook.presto.sql.optimizer.rule.UncorrelatedScalarToJoin;
import com.facebook.presto.sql.optimizer.tree.Expression;
import com.facebook.presto.sql.optimizer.tree.Let;
import com.facebook.presto.sql.optimizer.tree.Reference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Longs;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
                        new IntersectToUnion(),
                        new UncorrelatedScalarToJoin()
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
            explore(memo, lookup, new HashSet<>(), batch, rootClass);
        }

        Map<String, Expression> chosen = extract(rootClass, lookup);

        System.out.println(
                memo.toGraphviz(
                        e -> {
                            if (chosen.values().contains(e)) {
                                return ImmutableMap.of(
                                        "fillcolor", "coral",
                                        "style", "filled");
                            }

                            return ImmutableMap.of();
                        },
                        (from, to) -> {
                            if (chosen.values().contains(from) || chosen.values().contains(to)) {
                                return ImmutableMap.of(
                                        "color", "coral",
                                        "penwidth", "3");
                            }
                            return ImmutableMap.of();
                        }));

        return new Let(chosen, new Reference(rootClass));
    }

    private boolean explore(Memo memo, MemoLookup lookup, Set<String> explored, Set<Rule> rules, String group)
    {
        if (explored.contains(group)) {
            return false;
        }
        explored.add(group);

        Expression expression = lookup.lookup(new Reference(group))
                .findFirst()
                .get();

        boolean changed = false;

        boolean childrenChanged;
        boolean progress;
        do {
            Optional<Expression> rewritten = applyRules(rules, memo, lookup, expression);

            progress = false;
            if (rewritten.isPresent()) {
                progress = true;
                expression = rewritten.get();
            }

            childrenChanged = expression.getArguments().stream()
                    .map(Reference.class::cast)
                    .map(Reference::getName)
                    .map(name -> explore(memo, lookup.push(name), explored, rules, name))
                    .anyMatch(v -> v);

            changed = changed || progress || childrenChanged;
        }
        while (progress || childrenChanged);

        return changed;
    }

    private Optional<Expression> applyRules(Set<Rule> rules, Memo memo, MemoLookup lookup, Expression expression)
    {
        boolean changed = false;

        boolean progress;
        do {
            progress = false;
            for (Rule rule : rules) {
                List<Expression> transformed = rule.apply(expression, lookup)
                        .collect(Collectors.toList());

                checkState(transformed.size() <= 1, "Expected one expression");
                if (!transformed.isEmpty()) {
                    Optional<Expression> rewritten = memo.transform(expression, transformed.get(0), rule.getClass().getSimpleName());
                    if (rewritten.isPresent()) {
                        changed = true;
                        progress = true;
                        expression = rewritten.get();
                    }
                }
            }
        }
        while (progress);

        if (changed) {
            return Optional.of(expression);
        }

        return Optional.empty();
    }

    private Map<String, Expression> extract(String group, MemoLookup lookup)
    {
        Expression expression = lookup.lookup(new Reference(group))
                .findFirst()
                .get();

        return expression.getArguments().stream()
                .map(Reference.class::cast)
                .map(e -> extract(e.getName(), lookup.push(e.getName())))
                .reduce(ImmutableMap.of(group, expression), (accumulator, e) -> ImmutableMap.<String, Expression>builder().putAll(accumulator).putAll(e).build());
    }
}
