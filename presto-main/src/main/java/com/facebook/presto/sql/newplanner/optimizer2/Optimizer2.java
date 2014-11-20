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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import java.util.List;

import static com.facebook.presto.sql.newplanner.optimizer.PhysicalConstraints.GlobalPartitioning.PARTITIONED;
import static com.facebook.presto.sql.newplanner.optimizer.PhysicalConstraints.GlobalPartitioning.REPLICATED;
import static com.facebook.presto.sql.newplanner.optimizer.PhysicalConstraints.GlobalPartitioning.UNPARTITIONED;

public class Optimizer2
{
    public OptimizationResult optimize(RelExpr expression)
    {
        return optimize(expression, PhysicalConstraints.any(), new OptimizerContext2(expression.getId() + 1));
    }

    public OptimizationResult optimize(RelExpr expression, OptimizerContext2 context)
    {
        return optimize(expression, PhysicalConstraints.any(), context);
    }

    public OptimizationResult optimize(RelExpr expression, PhysicalConstraints requirements, OptimizerContext2 context)
    {
//        Optional<OptimizationResult> previous = context.getOptimized(expression, requirements);
//        if (previous.isPresent()) {
//            return previous.get();
//        }

        // handle implementable expressions...
        ImmutableList.Builder<OptimizedExpr> results = ImmutableList.builder();
        if (expression.getType() == RelExpr.Type.FILTER || expression.getType() == RelExpr.Type.PROJECT) {
            PhysicalConstraints childConstraints = PhysicalConstraints.any();

            OptimizationResult optimizedChild = optimize(expression.getInputs().get(0), childConstraints, context);
            for (OptimizedExpr child : optimizedChild.getAlternatives()) {
                OptimizedExpr result = new OptimizedExpr(expression.getId(), expression.getType(), child.getProperties(), ImmutableList.of(child), ImmutableList.of(childConstraints));
                results.add(enforceConstraints(requirements, result, context));
            }
        }
        else if (expression.getType() == RelExpr.Type.TABLE) {
            // TODO: pick best partitioning that satisfies requirements
            OptimizedExpr result = new OptimizedExpr(expression.getId(), expression.getType(), PhysicalProperties.partitioned((List<Integer>) expression.getPayload()), ImmutableList.<OptimizedExpr>of(), ImmutableList.<PhysicalConstraints>of());
            results.add(enforceConstraints(requirements, result, context));
        }
        else if (expression.getType() == RelExpr.Type.GROUPED_AGGREGATION) {
            // unpartitioned
            PhysicalConstraints childConstraint = PhysicalConstraints.unpartitioned();
            for (OptimizedExpr optimizedChild : optimize(expression.getInputs().get(0), childConstraint, context).getAlternatives()) {
                OptimizedExpr result = new OptimizedExpr(expression.getId(), expression.getType(), optimizedChild.getProperties(), ImmutableList.of(optimizedChild), ImmutableList.of(childConstraint));
                results.add(enforceConstraints(requirements, result, context));
            }

            // partitioned(k)
            childConstraint = PhysicalConstraints.partitioned((List<Integer>) expression.getPayload());
            for (OptimizedExpr optimizedChild : optimize(expression.getInputs().get(0), childConstraint, context).getAlternatives()) {
                // TODO derive output properties based on child properties
                PhysicalProperties deliveredProperties = optimizedChild.getProperties();
                OptimizedExpr result = new OptimizedExpr(expression.getId(), expression.getType(), deliveredProperties, ImmutableList.of(optimizedChild), ImmutableList.of(childConstraint));
                results.add(enforceConstraints(requirements, result, context));
            }
        }
        else if (expression.getType() == RelExpr.Type.LOCAL_GROUPED_AGGREGATION) {
            PhysicalConstraints childConstraint = PhysicalConstraints.any();
            for (OptimizedExpr optimizedChild : optimize(expression.getInputs().get(0), childConstraint, context).getAlternatives()) {
                PhysicalProperties deliveredProperties = optimizedChild.getProperties();
                OptimizedExpr result = new OptimizedExpr(expression.getId(), expression.getType(), deliveredProperties, ImmutableList.of(optimizedChild), ImmutableList.of(childConstraint));
                results.add(enforceConstraints(requirements, result, context));
            }
        }
        else if (expression.getType() == RelExpr.Type.HASH_JOIN) {
            RelExpr left = expression.getInputs().get(0);
            RelExpr right = expression.getInputs().get(1);

            // unpartitioned vs unpartitioned
            PhysicalConstraints leftConstraint = PhysicalConstraints.unpartitioned();
            PhysicalConstraints rightConstraint = PhysicalConstraints.unpartitioned();
            for (OptimizedExpr optimizedLeft : optimize(left, leftConstraint, context).getAlternatives()) {
                for (OptimizedExpr optimizedRight : optimize(right, rightConstraint, context).getAlternatives()) {
                    PhysicalProperties deliveredProperties = PhysicalProperties.unpartitioned();
                    OptimizedExpr result = new OptimizedExpr(expression.getId(), expression.getType(), deliveredProperties, ImmutableList.of(optimizedLeft, optimizedRight), ImmutableList.of(leftConstraint, rightConstraint));
                    results.add(enforceConstraints(requirements, result, context));
                }
            }

            // part(k) vs part(k)
            leftConstraint = PhysicalConstraints.partitioned((List<Integer>) expression.getPayload());
            rightConstraint = PhysicalConstraints.partitioned((List<Integer>) expression.getPayload());
            for (OptimizedExpr optimizedLeft : optimize(left, leftConstraint, context).getAlternatives()) {
                for (OptimizedExpr optimizedRight : optimize(right, rightConstraint, context).getAlternatives()) {
                    // TODO: infer partitioning properties from left/right
                    PhysicalProperties deliveredProperties = PhysicalProperties.partitioned((List<Integer>) expression.getPayload());
                    OptimizedExpr result = new OptimizedExpr(expression.getId(), expression.getType(), deliveredProperties, ImmutableList.of(optimizedLeft, optimizedRight), ImmutableList.of(leftConstraint, rightConstraint));
                    results.add(enforceConstraints(requirements, result, context));
                }
            }

            // part(*) vs replicated
            leftConstraint = PhysicalConstraints.partitionedAny();
            rightConstraint = PhysicalConstraints.replicated();
            for (OptimizedExpr optimizedLeft : optimize(left, leftConstraint, context).getAlternatives()) {
//            for (OptimizationResult optimizedLeft : optimize(left, PhysicalConstraints.partitioned((List<Integer>) expression.getPayload()), context)) {
                for (OptimizedExpr optimizedRight : optimize(right, rightConstraint, context).getAlternatives()) {
                    // TODO: infer partitioning properties from left/right
                    PhysicalProperties deliveredProperties = optimizedLeft.getProperties();
                    OptimizedExpr result = new OptimizedExpr(expression.getId(), expression.getType(), deliveredProperties, ImmutableList.of(optimizedLeft, optimizedRight), ImmutableList.of(leftConstraint, rightConstraint));
                    results.add(enforceConstraints(requirements, result, context));
                }
            }
        }
        else {
            throw new UnsupportedOperationException("Can't optimize: " + expression.getType());
        }

        List<OptimizedExpr> alternatives = results.build();
        OptimizedExpr best = Ordering.from(new CostComparator()).min(alternatives);

        OptimizationResult result = new OptimizationResult(best, alternatives);
        context.recordOptimization(expression, requirements, result);
        return result;
    }

    private OptimizedExpr enforceConstraints(PhysicalConstraints requirements, OptimizedExpr result, OptimizerContext2 context)
    {
        // no need to enforce
        if (!requirements.hasPartitioningConstraint()) {
            return result;
        }

        PhysicalProperties properties = result.getProperties();

        PhysicalConstraints.GlobalPartitioning constraint = requirements.getPartitioningConstraint().get();
        PhysicalConstraints.GlobalPartitioning actual = properties.getGlobalPartitioning();

        if (constraint == UNPARTITIONED) {
            if (actual == UNPARTITIONED) {
                return result;
            }
            else if (actual == PARTITIONED) {
                return new OptimizedExpr(context.nextId(), RelExpr.Type.MERGE, PhysicalProperties.unpartitioned(), ImmutableList.of(result), ImmutableList.of(PhysicalConstraints.any()));
            }
        }
        else if (constraint == REPLICATED) {
            return new OptimizedExpr(context.nextId(), RelExpr.Type.REPLICATE, PhysicalProperties.replicated(), ImmutableList.of(result), ImmutableList.of(PhysicalConstraints.any()));
        }
        else if (constraint == PARTITIONED && actual == PARTITIONED) {
            // req: partitioned:<any>
            if (!requirements.getPartitioningColumns().isPresent()) {
                return result;
            }
            else if (!requirements.getPartitioningColumns().get().equals(properties.getPartitioningColumns())) {
                return new OptimizedExpr(context.nextId(), RelExpr.Type.REPARTITION, PhysicalProperties.partitioned(requirements.getPartitioningColumns().get()), ImmutableList.of(result), ImmutableList.of(PhysicalConstraints.any()));
            }
            else {
                return result;
            }
        }
        else if (constraint == PARTITIONED && requirements.getPartitioningColumns().isPresent() && actual == UNPARTITIONED) {
            return new OptimizedExpr(context.nextId(), RelExpr.Type.REPARTITION, PhysicalProperties.partitioned(requirements.getPartitioningColumns().get()), ImmutableList.of(result), ImmutableList.of(PhysicalConstraints.any()));
        }
        else if (constraint == PARTITIONED && !requirements.getPartitioningColumns().isPresent() && actual == UNPARTITIONED) {
            return new OptimizedExpr(context.nextId(), RelExpr.Type.REPARTITION, PhysicalProperties.partitioned(ImmutableList.<Integer>of()), ImmutableList.of(result), ImmutableList.of(PhysicalConstraints.any()));
        }

        throw new UnsupportedOperationException(String.format("not yet implemented: required = %s, actual = %s", requirements, properties));
    }
}
