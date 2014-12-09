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
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.sql.newplanner.optimizer.PhysicalConstraints.GlobalPartitioning.PARTITIONED;
import static com.facebook.presto.sql.newplanner.optimizer.PhysicalConstraints.GlobalPartitioning.REPLICATED;
import static com.facebook.presto.sql.newplanner.optimizer.PhysicalConstraints.GlobalPartitioning.UNPARTITIONED;

public class Optimizer2
{
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

            ImmutableList.Builder<RelExpr> alternatives = ImmutableList.builder();

            // handle implementable expressions...
            if (expression.getType() == RelExpr.Type.FILTER) {
                PhysicalConstraints childConstraints = PhysicalConstraints.any();
                RelExpr child = optimize(expression.getInputs().get(0), childConstraints, context);

                OptimizationResult optimizedChild = (OptimizationResult) child.getPayload();
                RelExpr result = new RelExpr(expression.getId(), expression.getType(), ImmutableList.of(child), optimizedChild.getBest().getProperties().get());

                alternatives.add(result);
            }
            else if (expression.getType() == RelExpr.Type.PROJECT) {
                PhysicalConstraints childConstraints = PhysicalConstraints.any();
                RelExpr child = optimize(expression.getInputs().get(0), childConstraints, context);

                OptimizationResult optimizedChild = (OptimizationResult) child.getPayload();
                RelExpr result = new RelExpr(expression.getId(), expression.getType(), ImmutableList.of(child), optimizedChild.getBest().getProperties().get());

                alternatives.add(result);
            }
            else if (expression.getType() == RelExpr.Type.TABLE) {
                // TODO: pick best partitioning that satisfies requirements
                RelExpr result = new RelExpr(expression.getId(), expression.getType(), null, ImmutableList.of(), PhysicalProperties.partitioned((List<Integer>) expression.getPayload()));

                return makeOptimizationResult(expression, requirements, ImmutableList.of(result), context);
            }
            else if (expression.getType() == RelExpr.Type.GROUPED_AGGREGATION) {
                for (PhysicalConstraints childConstraint : Arrays.asList(PhysicalConstraints.unpartitioned(), PhysicalConstraints.partitioned((List<Integer>) expression.getPayload()))) {
                    RelExpr optimizedChild = optimize(expression.getInputs().get(0), childConstraint, context);
                    RelExpr alternative = new RelExpr(expression.getId(), expression.getType(), null, ImmutableList.of(optimizedChild), optimizedChild.getProperties().get());
                    alternatives.add(enforceConstraints(requirements, alternative, context));
                }
            }
            else if (expression.getType() == RelExpr.Type.LOCAL_GROUPED_AGGREGATION) {
                PhysicalConstraints childConstraint = PhysicalConstraints.any();
                RelExpr optimizedChild = optimize(expression.getInputs().get(0), childConstraint, context);
                PhysicalProperties deliveredProperties = optimizedChild.getProperties().get();
                RelExpr result = new RelExpr(expression.getId(), expression.getType(), ImmutableList.of(optimizedChild), deliveredProperties);

                alternatives.add(result);
            }
            else if (expression.getType() == RelExpr.Type.HASH_JOIN) {
                RelExpr left = expression.getInputs().get(0);
                RelExpr right = expression.getInputs().get(1);

                // unpartitioned vs unpartitioned
                {
                    PhysicalConstraints leftConstraint = PhysicalConstraints.unpartitioned();
                    PhysicalConstraints rightConstraint = PhysicalConstraints.unpartitioned();

                    RelExpr optimizedLeft = optimize(left, leftConstraint, context);
                    RelExpr optimizedRight = optimize(right, rightConstraint, context);

                    PhysicalProperties deliveredProperties = PhysicalProperties.unpartitioned();
                    RelExpr result = new RelExpr(expression.getId(), expression.getType(), ImmutableList.of(optimizedLeft, optimizedRight), deliveredProperties);
                    alternatives.add(result);
                }

                // part(k) vs part(k)
                {
                    PhysicalConstraints leftConstraint = PhysicalConstraints.partitioned((List<Integer>) expression.getPayload());
                    PhysicalConstraints rightConstraint = PhysicalConstraints.partitioned((List<Integer>) expression.getPayload());

                    RelExpr optimizedLeft = optimize(left, leftConstraint, context);
                    RelExpr optimizedRight = optimize(right, rightConstraint, context);

                    // TODO: infer partitioning properties from left/right
                    PhysicalProperties deliveredProperties = PhysicalProperties.partitioned((List<Integer>) expression.getPayload());
                    RelExpr result = new RelExpr(expression.getId(), expression.getType(), ImmutableList.of(optimizedLeft, optimizedRight), deliveredProperties);
                    alternatives.add(result);
                }

                // part(*) vs replicated
                {
                    PhysicalConstraints leftConstraint = PhysicalConstraints.partitionedAny();
                    PhysicalConstraints rightConstraint = PhysicalConstraints.replicated();

                    RelExpr optimizedLeft = optimize(left, leftConstraint, context);
                    RelExpr optimizedRight = optimize(right, rightConstraint, context);

                    // TODO: infer partitioning properties from left/right
                    PhysicalProperties deliveredProperties = PhysicalProperties.partitioned((List<Integer>) expression.getPayload());
                    RelExpr result = new RelExpr(expression.getId(), expression.getType(), ImmutableList.of(optimizedLeft, optimizedRight), deliveredProperties);
                    alternatives.add(result);
                }

            }
            else {
                throw new UnsupportedOperationException("Can't optimize: " + expression.getType());
            }

            return makeOptimizationResult(expression, requirements, alternatives.build(), context);
        }
        finally {
            context.decreaseDepth();
        }
    }

    private RelExpr makeOptimizationResult(RelExpr expression, PhysicalConstraints requirements, List<RelExpr> alternatives, OptimizerContext2 context)
    {
        RelExpr best = alternatives.stream().min(new CostComparator()).get();

        RelExpr result = new RelExpr(context.nextId(),
                RelExpr.Type.OPTIMIZE,
                new OptimizationResult(best, alternatives, requirements),
                ImmutableList.of(),
                best.getProperties().get());

        result = enforceConstraints(requirements, result, context);
        context.recordOptimization(expression, requirements, result);
        return result;
    }

    private RelExpr enforceConstraints(PhysicalConstraints requirements, RelExpr result, OptimizerContext2 context)
    {
        // no need to enforce
        if (!requirements.hasPartitioningConstraint()) {
            return result;
        }

        PhysicalProperties properties = result.getProperties().get();

        PhysicalConstraints.GlobalPartitioning constraint = requirements.getPartitioningConstraint().get();
        PhysicalConstraints.GlobalPartitioning actual = properties.getGlobalPartitioning();

        if (constraint == UNPARTITIONED) {
            if (actual == UNPARTITIONED) {
                return result;
            }
            else if (actual == PARTITIONED) {
                return new RelExpr(context.nextId(), RelExpr.Type.MERGE, ImmutableList.of(result), PhysicalProperties.unpartitioned());
            }
        }
        else if (constraint == REPLICATED) {
            return new RelExpr(context.nextId(), RelExpr.Type.REPLICATE, ImmutableList.of(result), PhysicalProperties.replicated());
        }
        else if (constraint == PARTITIONED && actual == PARTITIONED) {
            // req: partitioned:<any>
            if (!requirements.getPartitioningColumns().isPresent()) {
                return result;
            }
            else if (!requirements.getPartitioningColumns().get().equals(properties.getPartitioningColumns())) {
                return new RelExpr(context.nextId(), RelExpr.Type.REPARTITION, ImmutableList.of(result), PhysicalProperties.partitioned(requirements.getPartitioningColumns().get()));
            }
            else {
                return result;
            }
        }
        else if (constraint == PARTITIONED && requirements.getPartitioningColumns().isPresent() && actual == UNPARTITIONED) {
            return new RelExpr(context.nextId(), RelExpr.Type.REPARTITION, ImmutableList.of(result), PhysicalProperties.partitioned(requirements.getPartitioningColumns().get()));
        }
        else if (constraint == PARTITIONED && !requirements.getPartitioningColumns().isPresent() && actual == UNPARTITIONED) {
            return new RelExpr(context.nextId(), RelExpr.Type.REPARTITION, ImmutableList.of(result), PhysicalProperties.partitioned(ImmutableList.<Integer>of()));
        }

        throw new UnsupportedOperationException(String.format("not yet implemented: required = %s, actual = %s", requirements, properties));
    }
}
