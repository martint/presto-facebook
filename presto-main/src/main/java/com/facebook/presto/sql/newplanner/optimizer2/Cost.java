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

import com.google.common.base.Objects;

public class Cost
        implements Comparable<Cost>
{
    private final double value;

    public Cost(double value)
    {
        this.value = value;
    }

    public Cost scale(double factor)
    {
        return new Cost(value * factor);
    }

    public double getValue()
    {
        return value;
    }

    @Override
    public int compareTo(Cost o)
    {
        return Double.compare(value, o.value);
    }

    @Override
    public String toString()
    {
        return Double.toString(value);
    }
}
