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
package com.facebook.presto.sql.newplanner.optimizer2.rule;

import com.facebook.presto.sql.newplanner.optimizer.PhysicalConstraints;
import com.facebook.presto.sql.newplanner.optimizer.RelExpr;
import com.facebook.presto.sql.newplanner.optimizer2.OptimizerCallback;
import com.facebook.presto.sql.newplanner.optimizer2.PhysicalProperties;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class ImplementBroadcastJoin
        implements ImplementationRule
{
    @Override
    public List<RelExpr> optimize(RelExpr expression, PhysicalConstraints requirements, OptimizerCallback optimizer)
    {
        if (expression.getType() != RelExpr.Type.HASH_JOIN) {
            return ImmutableList.of();
        }

        RelExpr left = expression.getInputs().get(0);
        RelExpr right = expression.getInputs().get(1);

        RelExpr optimizedLeft = optimizer.optimize(left, PhysicalConstraints.partitionedAny());
        RelExpr optimizedRight = optimizer.optimize(right, PhysicalConstraints.replicated());

        // TODO: infer partitioning properties from left/right
        PhysicalProperties deliveredProperties = PhysicalProperties.partitioned((List<Integer>) expression.getPayload());
        RelExpr result = new RelExpr(expression.getId(), expression.getType(), ImmutableList.of(optimizedLeft, optimizedRight), deliveredProperties);

        return ImmutableList.of(result);
    }
}
