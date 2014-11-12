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

import com.facebook.presto.sql.newplanner.optimizer.PhysicalConstraints;
import com.facebook.presto.sql.newplanner.optimizer.RelExpr;

import java.util.List;

public class Optimizer2
{
    public RelExpr optimize(RelExpr expression)
    {
        return optimize(expression, PhysicalConstraints.unpartitioned(), new OptimizerContext2()).getExpression();
    }

    public OptimizationResult optimize(RelExpr expression, PhysicalConstraints requirements, OptimizerContext2 context)
    {
        // handle implementable expressions...
        OptimizationResult result;

        if (expression.getType() == RelExpr.Type.FILTER) {
            PhysicalConstraints childConstraints = PhysicalConstraints.any();

            context.recordOptimizationRequest(expression, requirements, expression.getInputs().get(0), childConstraints);
            OptimizationResult optimizedChild = optimize(expression.getInputs().get(0), childConstraints, context);

            result = new OptimizationResult(new RelExpr(context.nextId(), RelExpr.Type.FILTER, optimizedChild.getExpression()), optimizedChild.getProperties());
            result = enforceConstraints(requirements, result, context);
        }
        else if (expression.getType() == RelExpr.Type.PROJECT) {
            PhysicalConstraints childConstraints = PhysicalConstraints.any();
            OptimizationResult optimizedChild = optimize(expression.getInputs().get(0), childConstraints, context);

            result = new OptimizationResult(new RelExpr(context.nextId(), RelExpr.Type.PROJECT, optimizedChild.getExpression()), optimizedChild.getProperties());
            result = enforceConstraints(requirements, result, context);
        }
        else if (expression.getType() == RelExpr.Type.TABLE) {
            // TODO: pick best partitioning that satisfies requirements
            result = new OptimizationResult(expression, PhysicalProperties.randomPartitioned());
            result = enforceConstraints(requirements, result, context);
        }
        else if (expression.getType() == RelExpr.Type.GROUPED_AGGREGATION) {
            // TODO if requirements are unpartitioned, what should child reqs be? unpartitioned or partitioned(keys)
            // If we force child to be unpartitioned we may miss out on being able to do partitioned agg
            // If we force child to be partitioned(key), we may be forcing the child to do a repartition of an unpartitioned expression just to satisfy the reqs
            // Maybe we need to try both and pick the "best" alternative
            // Maybe GROUP_AGGREGATION is not implementable and we need a SINGLE_NODE_AGG vs PARTITIONED_AGG vs ...

            if (requirements.hasPartitioningConstraint())
            // option 1: require unpartitioned input
            {
                PhysicalConstraints childConstraints = PhysicalConstraints.unpartitioned();
                OptimizationResult optimizedChild = optimize(expression.getInputs().get(0), childConstraints, context);

                // TODO derive output properties based on child properties
                result = new OptimizationResult(expression, PhysicalProperties.randomPartitioned());
            }
            // option 2: require partition(k) input
            {
                PhysicalConstraints childConstraints = PhysicalConstraints.partitioned((List<Integer>) expression.getPayload());
                OptimizationResult optimizedChild = optimize(expression.getInputs().get(0), childConstraints, context);

                // TODO derive output properties based on child properties
                result = new OptimizationResult(expression, PhysicalProperties.randomPartitioned());
            }

            result = enforceConstraints(requirements, result, context);
        }
        else {
            throw new UnsupportedOperationException("Can't optimize: " + expression.getType());
        }

        return result;
    }

    private OptimizationResult enforceConstraints(PhysicalConstraints requirements, OptimizationResult result, OptimizerContext2 context)
    {
        PhysicalProperties properties = result.getProperties();
        RelExpr expression = result.getExpression();

        // no need to enforce
        if (!requirements.hasPartitioningConstraint()) {
            return result;
        }

        // unpartitioned -> unpartitioned
        else if (!requirements.getPartitioningColumns().isPresent() && !properties.isPartitioned()) {
            return result;
        }
        // random partition -> random partition
        else if (requirements.getPartitioningColumns().isPresent() &&
                requirements.getPartitioningColumns().get().isEmpty() &&
                properties.isPartitioned() &&
                properties.getPartitioningColumns().isEmpty()) {
            return result;
        }
        // any partitioned -> unpartitioned
        else if (!requirements.getPartitioningColumns().isPresent() && properties.isPartitioned()) {
            return new OptimizationResult(new RelExpr(context.nextId(), RelExpr.Type.MERGE, expression), PhysicalProperties.unpartitioned());
        }

        throw new UnsupportedOperationException(String.format("not yet implemented: required = %s, actual = %s", requirements, properties));
    }

    private static class OptimizationResult
    {
        private final RelExpr expression;
        private final PhysicalProperties properties;

        public OptimizationResult(RelExpr expression, PhysicalProperties properties)
        {
            this.expression = expression;
            this.properties = properties;
        }

        public RelExpr getExpression()
        {
            return expression;
        }

        public PhysicalProperties getProperties()
        {
            return properties;
        }

        @Override
        public String toString()
        {
            return expression.getType() + "[" + properties + "]";
        }
    }
}
