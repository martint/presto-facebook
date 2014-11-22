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

import java.util.List;

public class OptimizedExpr
{
    private final int expressionId;
    private final RelExpr.Type type;
    private final PhysicalProperties properties;

    private final List<OptimizationResult> inputs;

    public OptimizedExpr(int expressionId, RelExpr.Type type, List<OptimizationResult> inputs, PhysicalProperties properties)
    {
        this.expressionId = expressionId;
        this.type = type;
        this.inputs = inputs;
        this.properties = properties;
    }

    public RelExpr.Type getType()
    {
        return type;
    }

    public List<OptimizationResult> getInputs()
    {
        return inputs;
    }

    public PhysicalProperties getProperties()
    {
        return properties;
    }

    public int getId()
    {
        return expressionId;
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

        OptimizedExpr that = (OptimizedExpr) o;

        if (expressionId != that.expressionId) {
            return false;
        }
        if (!inputs.equals(that.inputs)) {
            return false;
        }
        if (!properties.equals(that.properties)) {
            return false;
        }
        if (type != that.type) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = expressionId;
        result = 31 * result + type.hashCode();
        result = 31 * result + properties.hashCode();
        result = 31 * result + inputs.hashCode();
        return result;
    }
}
