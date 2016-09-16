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

import com.facebook.presto.sql.optimizer.rule.GetToScan;
import com.facebook.presto.sql.optimizer.rule.LogicalToPhysicalFilter;
import com.facebook.presto.sql.optimizer.rule.MergePhysicalFilters;
import com.facebook.presto.sql.optimizer.rule.RemoveRedundantFilter;
import com.facebook.presto.sql.optimizer.tree.Assignment;
import com.facebook.presto.sql.optimizer.tree.Expression;
import com.facebook.presto.sql.optimizer.tree.Lambda;
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

import static com.facebook.presto.sql.optimizer.engine.Utils.getChildren;
import static com.facebook.presto.sql.optimizer.tree.Expressions.lambda;
import static com.facebook.presto.sql.optimizer.tree.Expressions.let;
import static com.facebook.presto.sql.optimizer.tree.Expressions.variable;
import static com.google.common.base.Preconditions.checkState;

public class GreedyOptimizer
        implements Optimizer
{
    private final boolean debug;

    private final List<Set<Rule>> batches;

    public GreedyOptimizer(boolean debug, List<Set<Rule>> batches)
    {
        this.debug = debug;
        this.batches = ImmutableList.copyOf(batches);
    }

    public GreedyOptimizer(boolean debug)
    {
        this(debug, ImmutableList.of(
                ImmutableSet.of(
                        new RemoveRedundantFilter()
                ),
                ImmutableSet.of(
                        new GetToScan(),
                        new LogicalToPhysicalFilter(),
                        new MergePhysicalFilters()
                )
        ));

//                ImmutableSet.of(
//                        new IntersectToUnion(),
//                        new UncorrelatedScalarToJoin()
//                ),
//                ImmutableSet.of(
//                        new RemoveIdentityProjection(),
//                        new PushFilterThroughProject(),
//                        new PushFilterThroughAggregation(),
//                        new PushFilterThroughUnion(),
//                        new PushFilterThroughSort(),
//                        new PushAggregationThroughUnion(),
//                        new CombineFilters(),
//                        new CombineGlobalLimits(),
//                        new CombineProjections(),
//                        new PushGlobalLimitThroughUnion(),
//                        new PushLocalLimitThroughUnion(),
//                        new PushLimitThroughProject(),
//                        new CombineUnions(),
//                        new OrderByLimitToTopN(),
//                        new PushGlobalTopNThroughUnion(),
//                        new PushLocalTopNThroughUnion()
//                ),
//                ImmutableSet.of(
//                        new CombineScanFilterProject(),
//                        new CombineFilterAndCrossJoin(),
//                        new GetToScan())));
//        ));
    }

    public Expression optimize(Expression expression)
    {
        Memo memo = new Memo(true);

        long rootClass = memo.insert(expression);

        System.out.println(memo.toGraphviz());
        memo.dump();

        MemoLookup lookup = new MemoLookup(
                memo,
                rootClass,
                expressions -> expressions.sorted((e1, e2) -> -Longs.compare(e1.getVersion(), e2.getVersion()))
                        .limit(1));

        for (Set<Rule> batch : batches) {
            explore(memo, lookup, new HashSet<>(), batch, rootClass);
        }

        System.out.println(memo.toGraphviz());

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

        return let(assignments, variable("$" + rootClass));
    }

    private boolean explore(Memo memo, MemoLookup lookup, Set<Long> explored, Set<Rule> rules, long group)
    {
        if (explored.contains(group)) {
            return false;
        }
        explored.add(group);

        Expression expression = lookup.resolve(new GroupReference(group))
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

            childrenChanged = getChildren(expression).stream()
                    .map(GroupReference.class::cast)
                    .map(GroupReference::getId)
                    .map(id -> explore(memo, lookup.push(id), explored, rules, id))
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
                if (debug) {
                    System.out.println("***** Rule: " + rule.getClass().getSimpleName() + " *****");
                    System.out.println(expression);
                    System.out.println();
                }

                List<Expression> transformed = rule.apply(expression, lookup)
                        .collect(Collectors.toList());

                if (debug) {
                    System.out.println("=>");
                    for (Expression e : transformed) {
                        System.out.println(e);
                    }
                }
                checkState(transformed.size() <= 1, "Expected one expression");
                if (!transformed.isEmpty()) {
                    Optional<Expression> rewritten = memo.transform(expression, transformed.get(0), rule.getClass().getSimpleName());

                    if (rewritten.isPresent()) {
                        changed = true;
                        progress = true;
                        expression = rewritten.get();

                        if (expression instanceof GroupReference) {
                            // This could happen if the expression was determined to be equivalent to its child (i.e., a no op such as a TRUE filter)
                            // In that case, find the active expression in the given group and continue processing
                            expression = lookup.resolve(expression)
                                    .findFirst()
                                    .get();
                        }

                        if (debug) {
                            System.out.println(memo.dump());
                            System.out.println();
                        }
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

    private List<Assignment> extract(long group, MemoLookup lookup)
    {
        Expression expression = lookup.resolve(new GroupReference(group))
                .findFirst()
                .get();

        List<Assignment> assignments = new ArrayList<>();

        if (expression instanceof Lambda) {
            Lambda lambda = (Lambda) expression;
            Expression body = lambda.getBody();

            if (body instanceof GroupReference) {
                GroupReference reference = (GroupReference) body;
                body = let(extract(reference.getId(), lookup.push(reference.getId())), reference);
            }

            expression = lambda(body);
        }
        else {
            getChildren(expression).stream()
                    .map(GroupReference.class::cast)
                    .map(e -> extract(e.getId(), lookup.push(e.getId())))
                    .flatMap(List::stream)
                    .forEach(a -> {
                        if (!assignments.contains(a)) { // TODO: potentially inefficient -- need an ordered set
                            assignments.add(a);
                        }
                    });
        }

        Assignment assignment = new Assignment("$" + group, expression);
        if (!assignments.contains(assignment)) {
            assignments.add(assignment);
        }

        return assignments;
    }
}
