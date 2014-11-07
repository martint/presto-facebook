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
import com.facebook.presto.sql.newplanner.expression.ProjectExpression;
import com.facebook.presto.sql.newplanner.expression.RelationalExpression;
import com.facebook.presto.sql.newplanner.optimizer.ExplorationRule;
import com.facebook.presto.sql.newplanner.optimizer.OptimizerContext;
import com.facebook.presto.sql.newplanner.optimizer.RelExpr;
import com.facebook.presto.sql.relational.RowExpression;
import com.google.common.base.Optional;

import java.util.List;

public class PushFilterThroughProjection
        implements ExplorationRule
{
    @Override
    public Optional<RelExpr> apply(RelExpr expression, OptimizerContext context)
    {
        if (expression.getType() != RelExpr.Type.FILTER || expression.getInputs().get(0).getType() != RelExpr.Type.PROJECT) {
            return Optional.absent();
        }

        RelExpr child = expression.getInputs().get(0);

        return Optional.of(new RelExpr(context.nextExpressionId(),
                RelExpr.Type.PROJECT,
                new RelExpr(context.nextExpressionId(),
                        RelExpr.Type.FILTER,
                        child.getInputs().get(0))));
    }
}
