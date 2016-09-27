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

import com.facebook.presto.sql.optimizer.rule.LogicalToPhysicalFilter;
import com.facebook.presto.sql.optimizer.rule.MergePhysicalFilters;
import com.facebook.presto.sql.optimizer.rule.MergeTransforms;
import com.facebook.presto.sql.optimizer.rule.ReduceLambda;
import com.facebook.presto.sql.optimizer.rule.RemoveRedundantFilter;
import com.facebook.presto.sql.optimizer.rule.RemoveRedundantProjections;
import com.facebook.presto.sql.optimizer.tree.Assignment;
import com.facebook.presto.sql.optimizer.tree.Expression;
import com.facebook.presto.sql.optimizer.tree.Lambda;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

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
                ),
                ImmutableSet.of(
//                        new GetToScan(),
                        new LogicalToPhysicalFilter(),
                        new MergePhysicalFilters()
                )
        ));
    }

    private static class MemoLookup
        implements Lookup
    {
        private final HeuristicPlannerMemo2 memo;

        public MemoLookup(HeuristicPlannerMemo2 memo)
        {
            this.memo = memo;
        }

        @Override
        public Stream<Expression> resolve(Expression expression)
        {
            return Stream.of(first(expression));
        }

        @Override
        public Expression first(Expression expression)
        {
            if (expression instanceof GroupReference) {
                return memo.getExpression(((GroupReference) expression).getId());
            }

            return expression;
        }
    }

    public Expression optimize(Expression expression)
    {
        HeuristicPlannerMemo2 memo = new HeuristicPlannerMemo2();
        long root = memo.insert(expression);

//        System.out.println(memo.toGraphviz());

        for (Set<Rule> batch : batches) {
            explore(memo, batch, root, new ArrayDeque<>());
        }

//        System.out.println(memo.toGraphviz());

        List<Assignment> assignments = extract(root, new MemoLookup(memo));

//        Set<Expression> chosen = assignments.stream()
//                .map(Assignment::getExpression)
//                .collect(Collectors.toSet());

//        System.out.println(
//                memo.toGraphviz()
//                memo.toGraphviz(
//                        e -> {
//                            if (chosen.contains(e)) {
//                                return ImmutableMap.of(
//                                        "fillcolor", "coral",
//                                        "style", "filled");
//                            }
//
//                            return ImmutableMap.of();
//                        },
//                        (from, to) -> {
//                            if (chosen.contains(from) || chosen.contains(to)) {
//                                return ImmutableMap.of(
//                                        "color", "coral",
//                                        "penwidth", "3");
//                            }
//                            return ImmutableMap.of();
//                        })
//        );

        return let(assignments, variable("$" + root));
    }

    private boolean explore(HeuristicPlannerMemo2 memo, Set<Rule> rules, long group, Deque<Long> groups)
    {
//        System.out.println("=== Exploring (" + groups + ") -> $" + group + " ===");

        boolean progress = false;
        boolean childrenChanged;
        do {
            childrenChanged = false;
            boolean changed;
            do {
                changed = false;
                Expression expression = memo.getExpression(group);

//                System.out.println("Considering: " + expression);
                for (Rule rule : rules) {
//                    System.out.println("Trying: " + rule.getClass().getSimpleName());
                    Optional<Expression> transformed = rule.apply(expression, new MemoLookup(memo))
                            .limit(1)
                            .findFirst();

                    if (transformed.isPresent()) {
                        memo.transform(expression, transformed.get(), rule.getClass().getSimpleName());
                        changed = true;
                        progress = true;
                        break;
                    }
                }
            }
            while (changed);

            Expression expression = memo.getExpression(group);
            for (Expression child : getChildren(expression)) {
                if (child instanceof GroupReference) {
                    Deque<Long> path = new ArrayDeque<>(groups);
                    path.add(group);
                    childrenChanged = childrenChanged || explore(memo, rules, ((GroupReference) child).getId(), path);
                    if (childrenChanged) {
                        progress = true;
                        break;
                    }
                }
            }
        }
        while (childrenChanged);


//        System.out.println("=== Done (" + groups + ") -> $" + group + " ===");
//        System.out.println(memo.toGraphviz());
//        System.out.println();

        return progress;
    }

    private List<Assignment> extract(long group, Lookup lookup)
    {
        Expression expression = lookup.first(new GroupReference(group));

        List<Assignment> assignments = new ArrayList<>();

        if (expression instanceof Lambda) {
            Lambda lambda = (Lambda) expression;
            Expression body = lambda.getBody();

            if (body instanceof GroupReference) {
                GroupReference reference = (GroupReference) body;
                body = let(extract(reference.getId(), lookup), reference);
            }

            expression = lambda(body);
        }
        else {
            getChildren(expression).stream()
                    .filter(GroupReference.class::isInstance)
                    .map(GroupReference.class::cast)
                    .map(e -> extract(e.getId(), lookup))
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
