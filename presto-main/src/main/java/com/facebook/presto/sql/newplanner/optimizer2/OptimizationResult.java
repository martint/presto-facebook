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
import com.google.common.collect.ImmutableList;

import java.util.List;

public class OptimizationResult
{
    private final int requestedExpressionId;
    private final OptimizedExpr best;
    private final List<OptimizedExpr> alternatives;
    private final PhysicalConstraints requestedProperties;
    private final int salt;

    public OptimizationResult(int requestedExpressionId, OptimizedExpr best, List<OptimizedExpr> alternatives, PhysicalConstraints requestedProperties)
    {
        this(requestedExpressionId, best, alternatives, requestedProperties, 0);
    }

    public OptimizationResult(int requestedExpressionId, OptimizedExpr best, List<OptimizedExpr> alternatives, PhysicalConstraints requestedProperties, int salt)
    {
        this.requestedExpressionId = requestedExpressionId;
        this.best = best;
        this.alternatives = alternatives;
        this.requestedProperties = requestedProperties;
        this.salt = salt;
    }

    public int getRequestedExpressionId()
    {
        return requestedExpressionId;
    }

    public OptimizedExpr getBest()
    {
        return best;
    }

    public List<OptimizedExpr> getAlternatives()
    {
        return alternatives;
    }

    public PhysicalConstraints getRequestedProperties()
    {
        return requestedProperties;
    }
}
