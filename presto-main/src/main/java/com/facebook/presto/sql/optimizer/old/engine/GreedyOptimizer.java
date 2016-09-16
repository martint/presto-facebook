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
import com.facebook.presto.sql.optimizer.old.rule.PushLocalLimitThroughUnion;
import com.facebook.presto.sql.optimizer.old.rule.PushLocalTopNThroughUnion;
import com.facebook.presto.sql.optimizer.old.rule.RemoveIdentityProjection;
import com.facebook.presto.sql.optimizer.old.rule.UncorrelatedScalarToJoin;
import com.facebook.presto.sql.optimizer.old.tree.Assignment;
import com.facebook.presto.sql.optimizer.old.tree.Expression;
import com.facebook.presto.sql.optimizer.old.tree.Let;
import com.facebook.presto.sql.optimizer.old.tree.Reference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Longs;

import java.util.ArrayList;
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
                        new CombineUnions(),
                        new OrderByLimitToTopN(),
                        new PushGlobalTopNThroughUnion(),
                        new PushLocalTopNThroughUnion()
                ),
                ImmutableSet.of(
                        new CombineScanFilterProject(),
                        new CombineFilterAndCrossJoin(),
                        new GetToScan())));
    }

    public Expression optimize(Expression expression)
    {
        Memo memo = new Memo(true);

        String rootClass = memo.insert(expression);

        System.out.println(memo.toGraphviz());

        MemoLookup lookup = new MemoLookup(
                memo,
                rootClass,
                expressions -> expressions.sorted((e1, e2) -> -Longs.compare(e1.getVersion(), e2.getVersion()))
                        .limit(1));

        for (Set<Rule> batch : batches) {
            explore(memo, lookup, new HashSet<>(), batch, rootClass);
        }

        List<Assignment> assignments = extract(rootClass, lookup);

        Set<Expression> chosen = assignments.stream()
                .map(Assignment::getExpression)
                .collect(Collectors.toSet());

        System.out.println(
                memo.toGraphviz(
                        e -> {
                            if (chosen.contains(e)) {
                                return ImmutableMap.of(
                                        "fillcolor", "coral",
                                        "style", "filled");
                            }

                            return ImmutableMap.of();
                        },
                        (from, to) -> {
                            if (chosen.contains(from) || chosen.contains(to)) {
                                return ImmutableMap.of(
                                        "color", "coral",
                                        "penwidth", "3");
                            }
                            return ImmutableMap.of();
                        })
        );

        System.out.println(memo.dump());

        return new Let(assignments, new Reference(rootClass));
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

    private List<Assignment> extract(String group, MemoLookup lookup)
    {
        Expression expression = lookup.lookup(new Reference(group))
                .findFirst()
                .get();

        List<Assignment> assignments = new ArrayList<>();
        expression.getArguments().stream()
                .map(Reference.class::cast)
                .map(e -> extract(e.getName(), lookup.push(e.getName())))
                .flatMap(List::stream)
                .forEach(a -> {
                    if (!assignments.contains(a)) { // TODO: potentially inefficient -- need an ordered set
                        assignments.add(a);
                    }
                });

        Assignment assignment = new Assignment(group, expression);
        if (!assignments.contains(assignment)) {
            assignments.add(assignment);
        }

        return assignments;
    }
}
