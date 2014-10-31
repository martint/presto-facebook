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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.newplanner.RelationalExpressionType;
import com.facebook.presto.sql.newplanner.expression.EquivalenceGroupReferenceExpression;
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

    public RelationalExpression optimize(RelationalExpression expression)
    {
        OptimizerContext context = new OptimizerContext(expression);
        RelationalExpression result = optimize(expression, ExpressionProperties.UNPARTITIONED, context);

        System.out.println(context.expressionsToGraphviz());
        return result;
    }

    public RelationalExpression optimize(RelationalExpression expression, ExpressionProperties requirements, OptimizerContext context)
    {
        List<RelationalExpression> logical = explore(expression, context);
        List<RelationalExpression> implementations = implement(logical, requirements, context);

        // TODO: pick optimal expression from implementations
        return new EquivalenceGroupReferenceExpression(context.nextExpressionId(), context.getGroup(expression), new RelationalExpressionType(ImmutableList.<Type>of()));
    }

    private List<RelationalExpression> implement(List<RelationalExpression> expressions, ExpressionProperties requirements, OptimizerContext context)
    {
        Queue<RelationalExpression> queue = new ArrayQueue<>();
        queue.addAll(expressions);

        List<RelationalExpression> result = new ArrayList<>();
        while (!queue.isEmpty()) {
            RelationalExpression current = queue.poll();

            for (ImplementationRule rule : implementationRules) {
                Optional<RelationalExpression> implementation = rule.implement(current, requirements, this, context);
                if (implementation.isPresent()) {
                    result.add(implementation.get());

                    context.recordImplementation(current, implementation.get(), requirements, rule);
                }
            }
        }
        return result;
    }

    private List<RelationalExpression> explore(RelationalExpression expression, OptimizerContext context)
    {
        Queue<RelationalExpression> queue = new ArrayQueue<>();
        queue.add(expression);
        context.recordExpression(expression);

        List<RelationalExpression> result = new ArrayQueue<>();
        while (!queue.isEmpty()) {
            RelationalExpression current = queue.poll();
            result.add(current);

            for (ExplorationRule rule : explorationRules) {
                Optional<RelationalExpression> transformed = rule.apply(current, context);
                if (transformed.isPresent()) {
                    queue.add(transformed.get());
                    context.recordLogicalTransform(current, transformed.get(), rule);
                }
            }
        }
        return result;
    }
}
