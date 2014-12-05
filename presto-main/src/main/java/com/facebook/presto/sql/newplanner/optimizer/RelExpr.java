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
package com.facebook.presto.sql.newplanner.optimizer;

import com.facebook.presto.sql.newplanner.optimizer2.PhysicalProperties;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

public class RelExpr
{
    private int id;
    private final List<RelExpr> inputs;
    private final Type type;
    private final Object payload;
    private final Optional<PhysicalProperties> properties;

    public Object getPayload()
    {
        return payload;
    }

    public enum Type {
        FILTER,
        PROJECT,
        TABLE,

        LOCAL_GROUPED_AGGREGATION,
        GROUPED_AGGREGATION,

        HASH_JOIN,

        MERGE,
        REPLICATE,
        REPARTITION,

        OPTIMIZE
    }

    public RelExpr(int id, Type type, List<RelExpr> inputs, PhysicalProperties properties)
    {
        this(id, type, null, inputs, properties);
    }


    public RelExpr(int id, Type type)
    {
        this(id, type, null, ImmutableList.<RelExpr>of(), null);
    }

    public RelExpr(int id, Type type, Object payload)
    {
        this(id, type, payload, ImmutableList.<RelExpr>of(), null);
    }

    public RelExpr(int id, Type type, RelExpr input)
    {
        this(id, type, null, input);
    }

    public RelExpr(int id, Type type, Object payload, RelExpr input)
    {
        this(id, type, payload, ImmutableList.of(input), null);
    }

    public RelExpr(int id, Type type, Object payload, List<RelExpr> inputs, PhysicalProperties properties)
    {
        this.id = id;
        this.type = type;
        this.inputs = inputs;
        this.payload = payload;
        this.properties = Optional.ofNullable(properties);
    }

    public int getId()
    {
        return id;
    }

    public List<RelExpr> getInputs()
    {
        return inputs;
    }

    public Type getType()
    {
        return type;
    }

    public Optional<PhysicalProperties> getProperties()
    {
        return properties;
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

        RelExpr relExpr = (RelExpr) o;

        if (id != relExpr.id) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return id;
    }

    @Override
    public String toString()
    {
        return type + "@" + id;
    }
}
