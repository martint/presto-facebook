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
import com.facebook.presto.sql.newplanner.optimizer2.EnforcementRule;
import com.facebook.presto.sql.newplanner.optimizer2.OptimizerContext2;
import com.facebook.presto.sql.newplanner.optimizer2.PhysicalProperties;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.facebook.presto.sql.newplanner.optimizer.PhysicalConstraints.GlobalPartitioning.PARTITIONED;
import static com.facebook.presto.sql.newplanner.optimizer.PhysicalConstraints.GlobalPartitioning.UNPARTITIONED;

public class EnforceUnpartitioned
        implements EnforcementRule
{
    @Override
    public Optional<RelExpr> enforce(PhysicalConstraints requirements, RelExpr expression, OptimizerContext2 context)
    {
        PhysicalConstraints.GlobalPartitioning constraint = requirements.getPartitioningConstraint().get();
        PhysicalConstraints.GlobalPartitioning actual = expression.getProperties().get().getGlobalPartitioning();

        if (constraint != UNPARTITIONED) {
            return Optional.empty();
        }

        if (actual == UNPARTITIONED) {
            return Optional.of(expression);
        }

        if (actual == PARTITIONED) {
            return Optional.of(new RelExpr(context.nextId(), RelExpr.Type.MERGE, ImmutableList.of(expression), PhysicalProperties.unpartitioned()));
        }

        return Optional.empty();
    }
}
