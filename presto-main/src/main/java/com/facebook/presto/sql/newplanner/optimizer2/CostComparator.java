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
import java.util.EnumSet;
import java.util.function.Predicate;

public class CostComparator
        implements Comparator<RelExpr>
{
    public int compare(RelExpr first, RelExpr second)
    {
        // partitioned always wins over unpartitioned
        if (first.getProperties().get().isPartitioned() && !second.getProperties().get().isPartitioned()) {
            return -1;
        }
        else if (!first.getProperties().get().isPartitioned() && second.getProperties().get().isPartitioned()) {
            return 1;
        }

        Predicate<RelExpr> isExchange = e -> EnumSet.of(RelExpr.Type.MERGE, RelExpr.Type.REPARTITION, RelExpr.Type.REPLICATE).contains(e.getType());
        int exchanges = Integer.compare(count(first, isExchange), count(second, isExchange));
        if (exchanges != 0) {
            return exchanges;
        }

        Predicate<RelExpr> isBroadcast = e -> e.getType() == RelExpr.Type.REPLICATE;
        return Integer.compare(count(first, isBroadcast), count(second, isBroadcast));
    }

    private int count(RelExpr expression, Predicate<RelExpr> condition)
    {
        int count = 0;

        if (expression.getType() == RelExpr.Type.OPTIMIZE) {
            count += count(((OptimizationResult) expression.getPayload()).getBest(), condition);
        }
        else if (condition.test(expression)) {
            count++;
        }

        for (RelExpr child : expression.getInputs()) {
            count += count(child, condition);
        }

        return count;
    }
}
