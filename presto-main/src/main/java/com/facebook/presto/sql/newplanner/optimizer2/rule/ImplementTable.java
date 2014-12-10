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

public class ImplementTable
        implements ImplementationRule
{
    @Override
    public List<RelExpr> optimize(RelExpr expression, PhysicalConstraints requirements, OptimizerCallback optimizer)
    {
        if (expression.getType() != RelExpr.Type.TABLE) {
            return ImmutableList.of();
        }

        // TODO: pick best partitioning that satisfies requirements
        return ImmutableList.of(new RelExpr(expression.getId(), expression.getType(), null, ImmutableList.of(), PhysicalProperties.partitioned((List<Integer>) expression.getPayload())));
    }
}
