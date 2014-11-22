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

import com.facebook.presto.sql.newplanner.optimizer.ExpressionWithRequirements;
import com.facebook.presto.sql.newplanner.optimizer.PhysicalConstraints;
import com.facebook.presto.sql.newplanner.optimizer.RelExpr;
import com.google.common.base.Optional;

import java.util.HashMap;
import java.util.Map;

public class OptimizerContext2
{
    private int nextId;
    private final Map<ExpressionWithRequirements, OptimizationResult> memoized = new HashMap<>();

    public OptimizerContext2(int nextId)
    {
        this.nextId = nextId;
    }

    public int nextId()
    {
        return nextId++;
    }

    public Optional<OptimizationResult> getOptimized(RelExpr expression, PhysicalConstraints requirements)
    {
        OptimizationResult result = memoized.get(new ExpressionWithRequirements(expression, requirements));
        return Optional.fromNullable(result);
    }

    public void recordOptimization(RelExpr expression, PhysicalConstraints constraints, OptimizationResult result)
    {
        memoized.put(new ExpressionWithRequirements(expression, constraints), result);
    }
}
