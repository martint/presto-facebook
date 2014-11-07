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

import com.facebook.presto.sql.newplanner.expression.FilterExpression;
import com.facebook.presto.sql.newplanner.expression.RelationalExpression;
import com.facebook.presto.sql.newplanner.optimizer.PhysicalConstraints;
import com.facebook.presto.sql.newplanner.optimizer.ImplementationRule;
import com.facebook.presto.sql.newplanner.optimizer.Optimizer;
import com.facebook.presto.sql.newplanner.optimizer.OptimizerContext;
import com.facebook.presto.sql.newplanner.optimizer.RelExpr;
import com.google.common.base.Optional;

public class ImplementFilterRule
        implements ImplementationRule
{
    @Override
    public Optional<RelExpr> implement(RelExpr expression, PhysicalConstraints requirements, Optimizer optimizer, OptimizerContext context)
    {
        if (expression.getType() != RelExpr.Type.FILTER) {
            return Optional.absent();
        }

        // derive child requirements given output requirements => should infer partitioning:<any>
        // optimize child with partitioning:<any>
        // derive output properties based on actual delivered properties
        // add enforcer if necessary

        RelExpr child = optimizer.optimize(expression.getInputs().get(0), PhysicalConstraints.any(), context);

        return Optional.<RelExpr>of(new RelExpr(context.nextExpressionId(), RelExpr.Type.FILTER, child));
    }
}
