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

import java.util.List;

public class OptimizationResult
{
    private final RelExpr best;
    private final List<RelExpr> alternatives;
    private final PhysicalConstraints requestedProperties;

    public OptimizationResult(RelExpr best, List<RelExpr> alternatives, PhysicalConstraints requestedProperties)
    {
        this.best = best;
        this.alternatives = alternatives;
        this.requestedProperties = requestedProperties;
    }

    public RelExpr getBest()
    {
        return best;
    }

    public List<RelExpr> getAlternatives()
    {
        return alternatives;
    }

    public PhysicalConstraints getRequestedProperties()
    {
        return requestedProperties;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        OptimizationResult that = (OptimizationResult) o;

        if (alternatives != null ? !alternatives.equals(that.alternatives) : that.alternatives != null) {
            return false;
        }
        if (best != null ? !best.equals(that.best) : that.best != null) {
            return false;
        }
        if (requestedProperties != null ? !requestedProperties.equals(that.requestedProperties) : that.requestedProperties != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = best != null ? best.hashCode() : 0;
        result = 31 * result + (alternatives != null ? alternatives.hashCode() : 0);
        result = 31 * result + (requestedProperties != null ? requestedProperties.hashCode() : 0);
        return result;
    }
}
