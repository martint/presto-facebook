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
package com.facebook.presto.sql.newplanner.optimizer2;

import com.facebook.presto.sql.newplanner.expression.Utils;
import com.facebook.presto.sql.newplanner.optimizer.PhysicalConstraints;
import com.facebook.presto.sql.newplanner.optimizer.RelExpr;
import com.facebook.presto.sql.newplanner.optimizer2.rule.EnforcePartitioned;
import com.facebook.presto.sql.newplanner.optimizer2.rule.EnforceReplicated;
import com.facebook.presto.sql.newplanner.optimizer2.rule.EnforceUnpartitioned;
import com.facebook.presto.sql.newplanner.optimizer2.rule.ImplementBroadcastJoin;
import com.facebook.presto.sql.newplanner.optimizer2.rule.ImplementFilter;
import com.facebook.presto.sql.newplanner.optimizer2.rule.ImplementGroupedAggregation;
import com.facebook.presto.sql.newplanner.optimizer2.rule.ImplementLocalGroupedAggregation;
import com.facebook.presto.sql.newplanner.optimizer2.rule.ImplementPartitionedJoin;
import com.facebook.presto.sql.newplanner.optimizer2.rule.ImplementProject;
import com.facebook.presto.sql.newplanner.optimizer2.rule.ImplementTable;
import com.facebook.presto.sql.newplanner.optimizer2.rule.ImplementUnpartitionedHashJoin;
import com.facebook.presto.sql.newplanner.optimizer2.rule.ImplementationRule;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;

public class Optimizer2
{
    private final List<ImplementationRule> implementationRules = ImmutableList.of(
            new ImplementBroadcastJoin(),
            new ImplementPartitionedJoin(),
            new ImplementUnpartitionedHashJoin(),
            new ImplementGroupedAggregation(),
            new ImplementLocalGroupedAggregation(),
            new ImplementProject(),
            new ImplementFilter(),
            new ImplementTable()
    );

    private final List<EnforcementRule> enforcementRules = ImmutableList.of(
            new EnforceUnpartitioned(),
            new EnforcePartitioned(),
            new EnforceReplicated()
    );

    public RelExpr optimize(RelExpr expression, PhysicalConstraints requirements, OptimizerContext2 context)
    {
        context.increaseDepth();
        try {
            System.out.println(String.format(Utils.indent(context.getDepth()) + "OPT(%s:%s, %s)", expression.getId(), expression.getType(), requirements));
            Optional<RelExpr> previous = context.getOptimized(expression, requirements);
            if (previous.isPresent()) {
                System.out.println(String.format(Utils.indent(context.getDepth()) + "  - memoized"));
                return previous.get();
            }

            ImmutableList.Builder<RelExpr> builder = ImmutableList.builder();

            for (ImplementationRule rule : implementationRules) {
                builder.addAll(rule.optimize(expression, requirements, (expr, constraints) -> optimize(expr, constraints, context)));
            }

            List<RelExpr> alternatives = builder.build();
            checkArgument(!alternatives.isEmpty(), "Expression is not implementable: %s", expression);

            List<RelExpr> enforced = alternatives.stream()
                    .map(e -> enforceConstraints(requirements, e, context))
                    .collect(toImmutableList());

            RelExpr best = enforced.stream().min(new CostComparator()).get();

            RelExpr result = new RelExpr(context.nextId(),
                    RelExpr.Type.OPTIMIZE,
                    new OptimizationResult(best, enforced, requirements),
                    ImmutableList.of(),
                    best.getProperties().get());

            context.recordOptimization(expression, requirements, result);
            return result;
        }
        finally {
            context.decreaseDepth();
        }
    }

    private RelExpr enforceConstraints(PhysicalConstraints requirements, RelExpr expression, OptimizerContext2 context)
    {
        // no need to enforce
        if (!requirements.hasPartitioningConstraint()) {
            return expression;
        }

        for (EnforcementRule rule : enforcementRules) {
            java.util.Optional<RelExpr> result = rule.enforce(requirements, expression, context);
            if (result.isPresent()) {
                return result.get();
            }
        }

        throw new UnsupportedOperationException(String.format("Constraint enforcement not implemented for %s with requirements %s", expression.getProperties().get(), requirements));
    }
}
