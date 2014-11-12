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
import com.facebook.presto.sql.newplanner.optimizer.graph.Graph;

public class OptimizerContext2
{
    private int nextId;

    private final Graph<String, String, String, String> optimizationSequence = new Graph<>();

    public int nextId()
    {
        return nextId++;
    }

    public void recordOptimizationRequest(String parent, String child)
    {
        optimizationSequence.addNode(parent, child);
    }

    public void recordOptimizationRequest(RelExpr parent, PhysicalConstraints requirements, RelExpr child, PhysicalConstraints childRequirements)
    {
//        optimizationSequence.addNode();
    }
}
