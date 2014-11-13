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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class RelExpr
{
    private int id;
    private final List<RelExpr> inputs;
    private final Type type;
    private final Object payload;

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
        PARTITION,

        OPTIMIZE
    }

    public RelExpr(int id, Type type)
    {
        this(id, type, null, ImmutableList.<RelExpr>of());
    }

    public RelExpr(int id, Type type, Object payload)
    {
        this(id, type, payload, ImmutableList.<RelExpr>of());
    }

    public RelExpr(int id, Type type, RelExpr input)
    {
        this(id, type, null, input);
    }

    public RelExpr(int id, Type type, Object payload, RelExpr input)
    {
        this(id, type, payload, ImmutableList.of(input));
    }

    public RelExpr(int id, Type type, Object payload, List<RelExpr> inputs)
    {
        this.id = id;
        this.type = type;
        this.inputs = inputs;
        this.payload = payload;
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

    @Override
    public String toString()
    {
        return type + "@" + id;
    }
}
