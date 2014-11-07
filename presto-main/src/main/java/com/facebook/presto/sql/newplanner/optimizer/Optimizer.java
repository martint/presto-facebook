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
package com.facebook.presto.sql.newplanner.optimizer;

import com.facebook.presto.sql.newplanner.expression.OptimizationRequestExpression;
import com.facebook.presto.sql.newplanner.expression.RelationalExpression;
import com.facebook.presto.sql.newplanner.optimizer.rules.ImplementAggregationRule;
import com.facebook.presto.sql.newplanner.optimizer.rules.ImplementFilterRule;
import com.facebook.presto.sql.newplanner.optimizer.rules.ImplementProjectionRule;
import com.facebook.presto.sql.newplanner.optimizer.rules.ImplementTableScanRule;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.eclipse.jetty.util.ArrayQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class Optimizer
{
    private final List<ImplementationRule> implementationRules = ImmutableList.of(
            new ImplementFilterRule(),
            new ImplementProjectionRule(),
            new ImplementTableScanRule(),
            new ImplementAggregationRule()
    );

    private final List<ExplorationRule> explorationRules = ImmutableList.<ExplorationRule>of(
//            new PushFilterThroughProjection()
    );

    public RelExpr optimize(RelExpr expression)
    {
        OptimizerContext context = new OptimizerContext(expression);
        RelExpr result = optimize(expression, PhysicalConstraints.unpartitioned(), context);

        System.out.println(context.expressionsToGraphviz());
        return result;
    }

    public RelExpr optimize(RelExpr expression, PhysicalConstraints requirements, OptimizerContext context)
    {
        Optional<RelExpr> optimized = context.getOptimized(expression, requirements);
        if (optimized.isPresent()) {
            return optimized.get();
        }

        List<RelExpr> logical = explore(expression, context);

        // context.getGroup(expression), requirements
        RelExpr result = new RelExpr(context.nextExpressionId(), RelExpr.Type.OPTIMIZE);
        context.recordOptimization(expression, requirements, result);

        List<RelExpr> implementations = implement(logical, requirements, context);

        // TODO: pick optimal expression from implementations

        return result;
    }

    private List<RelExpr> implement(List<RelExpr> expressions, PhysicalConstraints requirements, OptimizerContext context)
    {
        Queue<RelExpr> queue = new ArrayQueue<>();
        queue.addAll(expressions);

        List<RelExpr> result = new ArrayList<>();
        while (!queue.isEmpty()) {
            RelExpr current = queue.poll();

            for (ImplementationRule rule : implementationRules) {
                Optional<RelExpr> implementation = rule.implement(current, requirements, this, context);
                if (implementation.isPresent()) {
                    result.add(implementation.get());

                    context.recordImplementation(current, implementation.get(), requirements, rule);
                }
            }
        }
        return result;
    }

    private List<RelExpr> explore(RelExpr expression, OptimizerContext context)
    {
        Queue<RelExpr> queue = new ArrayQueue<>();
        queue.add(expression);
        context.recordExpression(expression);

        List<RelExpr> result = new ArrayQueue<>();
        while (!queue.isEmpty()) {
            RelExpr current = queue.poll();
            result.add(current);

            for (ExplorationRule rule : explorationRules) {
                Optional<RelExpr> transformed = rule.apply(current, context);
                if (transformed.isPresent()) {
                    queue.add(transformed.get());
                    context.recordLogicalTransform(current, transformed.get(), rule);
                }
            }
        }
        return result;
    }
}
