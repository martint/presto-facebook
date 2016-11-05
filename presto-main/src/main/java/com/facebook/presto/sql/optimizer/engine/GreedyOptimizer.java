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
import com.facebook.presto.sql.optimizer.rule.ReduceTransform;
import com.facebook.presto.sql.optimizer.rule.RemoveRedundantFilter;
import com.facebook.presto.sql.optimizer.rule.RemoveRedundantProjections;
import com.facebook.presto.sql.optimizer.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.optimizer.engine.Utils.getChildren;

public class GreedyOptimizer
//        implements Optimizer
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
                        new RemoveRedundantProjections(),
                        new ReduceTransform()
                ),
                ImmutableSet.of(
                        new LogicalToPhysicalFilter(),
                        new MergePhysicalFilters()
                )
        ));
    }

    public static class MemoLookup
        implements Lookup
    {
        private final HeuristicPlannerMemo memo;

        public MemoLookup(HeuristicPlannerMemo memo)
        {
            this.memo = memo;
        }

        @Override
        public Expression resolve(Expression expression)
        {
            if (expression instanceof GroupReference) {
                return memo.getExpression(((GroupReference) expression).getId());
            }

            return expression;
        }
    }

    public HeuristicPlannerMemo optimize(Expression expression)
    {
        HeuristicPlannerMemo memo = new HeuristicPlannerMemo(expression);
        System.out.println(memo);
        System.out.println();
        long root = memo.getRoot();

        for (Set<Rule> batch : batches) {
            explore(memo, batch, root, new ArrayDeque<>());
        }

//        List<Assignment> assignments = extract(root, new MemoLookup(memo));

        return memo;
    }

    private boolean explore(HeuristicPlannerMemo memo, Set<Rule> rules, long group, Deque<Long> groups)
    {
        boolean progress = false;
        boolean childrenChanged;
        do {
            childrenChanged = false;
            boolean changed;
            do {
                changed = false;
                Expression expression = memo.getExpression(group);

                for (Rule rule : rules) {
                    Optional<Expression> transformed = rule.transform(expression, new MemoLookup(memo))
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

        return progress;
    }

//    private List<Assignment> extract(long group, Lookup lookup)
//    {
//        Expression expression = lookup.resolve(new GroupReference(null, group));
//
//        List<Assignment> assignments = new ArrayList<>();
//
//        if (expression instanceof Lambda) {
//            Lambda lambda = (Lambda) expression;
//            Expression body = lambda.getBody();
//
//            if (body instanceof GroupReference) {
//                GroupReference reference = (GroupReference) body;
//                body = let(extract(reference.getId(), lookup), reference);
//            }
//
//            expression = lambda(body);
//        }
//        else {
//            getChildren(expression).stream()
//                    .filter(GroupReference.class::isInstance)
//                    .map(GroupReference.class::cast)
//                    .map(e -> extract(e.getId(), lookup))
//                    .flatMap(List::stream)
//                    .forEach(a -> {
//                        if (!assignments.contains(a)) { // TODO: potentially inefficient -- need an ordered set
//                            assignments.add(a);
//                        }
//                    });
//        }
//
//        Assignment assignment = new Assignment("$" + group, expression);
//        if (!assignments.contains(assignment)) {
//            assignments.add(assignment);
//        }
//
//        return assignments;
//    }
}
