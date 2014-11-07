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
package com.facebook.presto.sql.newplanner.optimizer.rules;

import com.facebook.presto.sql.newplanner.optimizer.ImplementationRule;
import com.facebook.presto.sql.newplanner.optimizer.Optimizer;
import com.facebook.presto.sql.newplanner.optimizer.OptimizerContext;
import com.facebook.presto.sql.newplanner.optimizer.PhysicalConstraints;
import com.facebook.presto.sql.newplanner.optimizer.RelExpr;
import com.google.common.base.Optional;

public class ImplementTableScanRule
    implements ImplementationRule
{
    @Override
    public Optional<RelExpr> implement(RelExpr expression, PhysicalConstraints requirements, Optimizer optimizer, OptimizerContext context)
    {
        if (expression.getType() != RelExpr.Type.TABLE) {
            return Optional.absent();
        }

        // TODO requirements
        return Optional.of(new RelExpr(context.nextExpressionId(), RelExpr.Type.TABLE));
    }
}
