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
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class Optimizer2
{
    public List<OptimizationResult> optimize(RelExpr expression)
    {
        return optimize(expression, PhysicalConstraints.unpartitioned(), new OptimizerContext2(expression.getId() + 1));
    }

    public List<OptimizationResult> optimize(RelExpr expression, PhysicalConstraints requirements, OptimizerContext2 context)
    {
        Optional<List<OptimizationResult>> previous = context.getOptimized(expression, requirements);
        if (previous.isPresent()) {
            return previous.get();
        }

        // handle implementable expressions...
        ImmutableList.Builder<OptimizationResult> results = ImmutableList.builder();

        if (expression.getType() == RelExpr.Type.FILTER || expression.getType() == RelExpr.Type.PROJECT) {
            PhysicalConstraints childConstraints = PhysicalConstraints.any();

            List<OptimizationResult> optimizedChild = optimize(expression.getInputs().get(0), childConstraints, context);
            for (OptimizationResult child : optimizedChild) {
                OptimizationResult result = new OptimizationResult(expression.getId(), expression.getType(), child.getProperties(), ImmutableList.of(child));
                results.add(enforceConstraints(requirements, result, context));
            }
        }
        else if (expression.getType() == RelExpr.Type.TABLE) {
            // TODO: pick best partitioning that satisfies requirements
            OptimizationResult result = new OptimizationResult(expression.getId(), expression.getType(), PhysicalProperties.randomPartitioned(), ImmutableList.<OptimizationResult>of());
            results.add(enforceConstraints(requirements, result, context));

            // simulate table partitioned by some field
            results.add(enforceConstraints(requirements, new OptimizationResult(expression.getId(), expression.getType(), PhysicalProperties.partitioned(ImmutableList.of(1)), ImmutableList.<OptimizationResult>of()), context));
        }
        else if (expression.getType() == RelExpr.Type.GROUPED_AGGREGATION) {
            List<OptimizationResult> optimizedChidren = optimize(expression.getInputs().get(0), PhysicalConstraints.unpartitioned(), context);
            for (OptimizationResult optimizedChild : optimizedChidren) {
                OptimizationResult result = new OptimizationResult(expression.getId(), expression.getType(), optimizedChild.getProperties(), ImmutableList.of(optimizedChild));
                results.add(enforceConstraints(requirements, result, context));
            }

            List<OptimizationResult> optimizedChildren = optimize(expression.getInputs().get(0), PhysicalConstraints.partitioned((List<Integer>) expression.getPayload()), context);
            for (OptimizationResult optimizedChild : optimizedChildren) {
                // TODO derive output properties based on child properties
                PhysicalProperties deliveredProperties = optimizedChild.getProperties();
                OptimizationResult result = new OptimizationResult(expression.getId(), expression.getType(), deliveredProperties, ImmutableList.of(optimizedChild));
                results.add(enforceConstraints(requirements, result, context));
            }
        }
        else if (expression.getType() == RelExpr.Type.LOCAL_GROUPED_AGGREGATION) {
            for (OptimizationResult optimizedChild : optimize(expression.getInputs().get(0), PhysicalConstraints.any(), context)) {
                PhysicalProperties deliveredProperties = optimizedChild.getProperties();
                OptimizationResult result = new OptimizationResult(expression.getId(), expression.getType(), deliveredProperties, ImmutableList.of(optimizedChild));
                results.add(enforceConstraints(requirements, result, context));
            }
        }
        else {
            throw new UnsupportedOperationException("Can't optimize: " + expression.getType());
        }

        List<OptimizationResult> result = results.build();

        context.recordOptimization(expression, requirements, result);
        return result;
    }

    private OptimizationResult enforceConstraints(PhysicalConstraints requirements, OptimizationResult result, OptimizerContext2 context)
    {
        // no need to enforce
        if (!requirements.hasPartitioningConstraint()) {
            return result;
        }

        PhysicalProperties properties = result.getProperties();

        // unpartitioned -> unpartitioned
        if (!requirements.getPartitioningColumns().isPresent() && !properties.isPartitioned()) {
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
            return new OptimizationResult(context.nextId(), RelExpr.Type.MERGE, PhysicalProperties.unpartitioned(), ImmutableList.of(result));
        }
        // unpartitioned -> partitioned
        else if (requirements.getPartitioningColumns().isPresent() && !properties.isPartitioned()) {
            return new OptimizationResult(context.nextId(), RelExpr.Type.PARTITION, PhysicalProperties.partitioned(requirements.getPartitioningColumns().get()), ImmutableList.of(result));
        }
        // partitioned(k) -> partitioned(y)
        else if (requirements.getPartitioningColumns().isPresent() &&
                properties.isPartitioned() &&
                !requirements.getPartitioningColumns().get().equals(properties.getPartitioningColumns())) {
            return new OptimizationResult(context.nextId(), RelExpr.Type.PARTITION, PhysicalProperties.partitioned(requirements.getPartitioningColumns().get()), ImmutableList.of(result));
        }
        // partitioned(k) -> partitioned(k)
        else if (requirements.getPartitioningColumns().isPresent() &&
                properties.isPartitioned() &&
                requirements.getPartitioningColumns().get().equals(properties.getPartitioningColumns())) {
            return result;
        }

        throw new UnsupportedOperationException(String.format("not yet implemented: required = %s, actual = %s", requirements, properties));
    }
}
