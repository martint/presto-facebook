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

import com.facebook.presto.sql.newplanner.optimizer.RelExpr;

import java.util.Comparator;

public class CostComparator
    implements Comparator<OptimizationResult>
{
    public int compare(OptimizationResult first, OptimizationResult second)
    {
        // partitioned always wins over unpartitioned
        if (first.getProperties().isPartitioned() && !second.getProperties().isPartitioned()) {
            return -1;
        }
        else if (!first.getProperties().isPartitioned() && second.getProperties().isPartitioned()) {
            return 1;
        }
        else {
            return Integer.compare(countExchanges(first), countExchanges(second));
        }
    }

    private int countExchanges(OptimizationResult expression)
    {
        int count = 0;
        for (OptimizationResult child : expression.getInputs()) {
            count += countExchanges(child);
        }

        if (expression.getType() == RelExpr.Type.MERGE || expression.getType() == RelExpr.Type.PARTITION) {
            count++;
        }

        return count;
    }
}
