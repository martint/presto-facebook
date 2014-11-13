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

import static com.facebook.presto.sql.newplanner.optimizer.PhysicalConstraints.GlobalPartitioning.PARTITIONED;
import static com.facebook.presto.sql.newplanner.optimizer.PhysicalConstraints.GlobalPartitioning.REPLICATED;
import static com.facebook.presto.sql.newplanner.optimizer.PhysicalConstraints.GlobalPartitioning.UNPARTITIONED;

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
            OptimizationResult result = new OptimizationResult(expression.getId(), expression.getType(), PhysicalProperties.partitioned((List<Integer>) expression.getPayload()), ImmutableList.<OptimizationResult>of());
            results.add(enforceConstraints(requirements, result, context));
        }
        else if (expression.getType() == RelExpr.Type.GROUPED_AGGREGATION) {
            // unpartitioned
            for (OptimizationResult optimizedChild : optimize(expression.getInputs().get(0), PhysicalConstraints.unpartitioned(), context)) {
                OptimizationResult result = new OptimizationResult(expression.getId(), expression.getType(), optimizedChild.getProperties(), ImmutableList.of(optimizedChild));
                results.add(enforceConstraints(requirements, result, context));
            }

            // partitioned(k)
            for (OptimizationResult optimizedChild : optimize(expression.getInputs().get(0), PhysicalConstraints.partitioned((List<Integer>) expression.getPayload()), context)) {
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
        else if (expression.getType() == RelExpr.Type.HASH_JOIN) {
            RelExpr left = expression.getInputs().get(0);
            RelExpr right = expression.getInputs().get(1);

            // unpartitioned vs unpartitioned
            for (OptimizationResult optimizedLeft : optimize(left, PhysicalConstraints.unpartitioned(), context)) {
                for (OptimizationResult optimizedRight : optimize(right, PhysicalConstraints.unpartitioned(), context)) {
                    PhysicalProperties deliveredProperties = PhysicalProperties.unpartitioned();
                    OptimizationResult result = new OptimizationResult(expression.getId(), expression.getType(), deliveredProperties, ImmutableList.of(optimizedLeft, optimizedRight));
                    results.add(enforceConstraints(requirements, result, context));
                }
            }

            // part(k) vs part(k)
            for (OptimizationResult optimizedLeft : optimize(left, PhysicalConstraints.partitioned((List<Integer>) expression.getPayload()), context)) {
                for (OptimizationResult optimizedRight : optimize(right, PhysicalConstraints.partitioned((List<Integer>) expression.getPayload()), context)) {
                    // TODO: infer partitioning properties from left/right
                    PhysicalProperties deliveredProperties = PhysicalProperties.partitioned((List<Integer>) expression.getPayload());
                    OptimizationResult result = new OptimizationResult(expression.getId(), expression.getType(), deliveredProperties, ImmutableList.of(optimizedLeft, optimizedRight));
                    results.add(enforceConstraints(requirements, result, context));
                }
            }

            // part(*) vs replicated
            for (OptimizationResult optimizedLeft : optimize(left, PhysicalConstraints.partitionedAny(), context)) {
//            for (OptimizationResult optimizedLeft : optimize(left, PhysicalConstraints.partitioned((List<Integer>) expression.getPayload()), context)) {
                for (OptimizationResult optimizedRight : optimize(right, PhysicalConstraints.replicated(), context)) {
                    // TODO: infer partitioning properties from left/right
                    PhysicalProperties deliveredProperties = optimizedLeft.getProperties();
                    OptimizationResult result = new OptimizationResult(expression.getId(), expression.getType(), deliveredProperties, ImmutableList.of(optimizedLeft, optimizedRight));
                    results.add(enforceConstraints(requirements, result, context));
                }
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

        PhysicalConstraints.GlobalPartitioning constraint = requirements.getPartitioningConstraint().get();
        PhysicalConstraints.GlobalPartitioning actual = properties.getGlobalPartitioning();
        if (constraint == UNPARTITIONED && actual == UNPARTITIONED) {
            return result;
        }
        // any partition -> any partition
        else if (constraint == PARTITIONED && !requirements.getPartitioningColumns().isPresent() &&
                actual == PARTITIONED) {
            return result;
        }
        // any partitioned -> unpartitioned
        else if (constraint == UNPARTITIONED && actual == PARTITIONED) {
            return new OptimizationResult(context.nextId(), RelExpr.Type.MERGE, PhysicalProperties.unpartitioned(), ImmutableList.of(result));
        }
        // partitioned(k) -> partitioned(y)
        else if (constraint == PARTITIONED && actual == PARTITIONED &&
                requirements.getPartitioningColumns().isPresent() &&
                !requirements.getPartitioningColumns().get().equals(properties.getPartitioningColumns())) {
            return new OptimizationResult(context.nextId(), RelExpr.Type.PARTITION, PhysicalProperties.partitioned(requirements.getPartitioningColumns().get()), ImmutableList.of(result));
        }
        // partitioned(k) -> partitioned(k)
        else if (constraint == PARTITIONED && actual == PARTITIONED &&
                requirements.getPartitioningColumns().isPresent() &&
                requirements.getPartitioningColumns().get().equals(properties.getPartitioningColumns())) {
            return result;
        }
        else if (constraint == REPLICATED) {
            return new OptimizationResult(context.nextId(), RelExpr.Type.REPLICATE, PhysicalProperties.replicated(), ImmutableList.of(result));
        }

        throw new UnsupportedOperationException(String.format("not yet implemented: required = %s, actual = %s", requirements, properties));
    }
}
