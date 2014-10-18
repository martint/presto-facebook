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
package com.facebook.presto.sql.newplanner.expression;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.newplanner.RelationalExpressionType;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class RelationalExpression
{
    private final int id;
    private final List<RelationalExpression> inputs;
    private final RelationalExpressionType type;

    // TODO: traits or physical properties

    public RelationalExpression(int id, RelationalExpressionType type, List<RelationalExpression> inputs)
    {
        checkNotNull(type, "type is null");
        checkNotNull(inputs, "inputs is null");

        this.id = id;
        this.inputs = inputs;
        this.type = type;
    }

    public final int getId()
    {
        return id;
    }

    public final RelationalExpressionType getType()
    {
        return type;
    }

    public final List<RelationalExpression> getInputs()
    {
        return inputs;
    }

    public static RelationalExpressionType concat(RelationalExpressionType left, RelationalExpressionType right)
    {
        List<Type> types = ImmutableList.<Type>builder()
                .addAll(left.getRowType())
                .addAll(right.getRowType())
                .build();

        return new RelationalExpressionType(types);
    }

    public abstract String toStringTree(int indent);
}
