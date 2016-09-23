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

import com.facebook.presto.sql.optimizer.rule.MergeTransforms;
import com.facebook.presto.sql.optimizer.rule.ReduceLambda;
import com.facebook.presto.sql.optimizer.rule.RemoveRedundantFilter;
import com.facebook.presto.sql.optimizer.rule.RemoveRedundantProjections;
import com.facebook.presto.sql.optimizer.tree.Assignment;
import com.facebook.presto.sql.optimizer.tree.Expression;
import com.facebook.presto.sql.optimizer.tree.Lambda;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.optimizer.engine.Utils.getChildren;
import static com.facebook.presto.sql.optimizer.tree.Expressions.lambda;
import static com.facebook.presto.sql.optimizer.tree.Expressions.let;
import static com.facebook.presto.sql.optimizer.tree.Expressions.variable;

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
                        new RemoveRedundantFilter(),
                        new MergeTransforms(),
                        new ReduceLambda(),
                        new RemoveRedundantProjections()
                )
//                ImmutableSet.of(
//                        new GetToScan(),
//                        new LogicalToPhysicalFilter(),
//                        new MergePhysicalFilters()
//                )
        ));
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
                expressions -> expressions);
        // expressions.sorted((e1, e2) -> -Longs.compare(e1.getVersion(), e2.getVersion()))
        //                .limit(1)

        for (Set<Rule> batch : batches) {
            explore(memo, batch, rootClass, new ArrayDeque<>());
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

    private boolean explore(Memo memo, Set<Rule> rules, long group, Deque<Long> groups)
    {
        System.out.println("=== Exploring (" + groups + ") -> $" + group + " ===");

        boolean progress = false;
        boolean anyChanges;
        do {
            if (!memo.isValid(group)) {
                return progress;
            }

            anyChanges = false;

            Expression expression = getExpression(memo, group);

            System.out.println("Considering: " + expression);
            boolean changed = false;
            for (Rule rule : rules) {
                System.out.println("Trying: " + rule.getClass().getSimpleName());
                Optional<Expression> transformed = rule.apply(expression, new MemoLookup(memo, group))
                        .limit(1)
                        .findFirst();

                if (!transformed.isPresent()) {
                    continue;
                }

                changed = memo.transform(expression, transformed.get(), rule.getClass().getSimpleName());
                if (changed) {
                    anyChanges = true;
                    progress = true;
                    break;
                }
            }

            if (!changed) {
                for (Expression child : getChildren(expression)) {
                    if (child instanceof GroupReference) {
                        Deque<Long> path = new ArrayDeque<>(groups);
                        path.add(group);
                        boolean childChanged = explore(memo, rules, ((GroupReference) child).getId(), path);
                        if (childChanged) {
                            anyChanges = true;
                            break;
                        }
                    }
                }
            }
        }
        while (anyChanges);


        System.out.println("=== Done (" + groups + ") -> $" + group + " ===");
        System.out.println(memo.toGraphviz());
        System.out.println();

        return progress;
    }

    private Expression getExpression(Memo memo, long group)
    {
        return memo.getExpressions(group).stream()
                .findFirst()
                .get();
    }

    private List<Assignment> extract(long group, MemoLookup lookup)
    {
        Expression expression = lookup.first(new GroupReference(group));

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
                    .filter(GroupReference.class::isInstance)
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
