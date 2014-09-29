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
import com.google.common.base.Preconditions;

import java.util.List;

import static com.google.common.base.Preconditions.*;

public class RelationalExpression
{
    private final int id;
    private final List<RelationalExpression> inputs;
    private final List<Type> rowType;

    // TODO: traits or physical properties

    public RelationalExpression(int id, List<Type> rowType, List<RelationalExpression> inputs)
    {
        checkNotNull(rowType, "rowType is null");
        checkNotNull(inputs, "inputs is null");

        this.id = id;
        this.inputs = inputs;
        this.rowType = rowType;
    }

    public int getId()
    {
        return id;
    }

    public List<Type> getRowType()
    {
        return rowType;
    }

    public List<RelationalExpression> getInputs()
    {
        return inputs;
    }
}
