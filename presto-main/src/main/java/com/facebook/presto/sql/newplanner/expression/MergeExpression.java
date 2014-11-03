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

import com.facebook.presto.sql.newplanner.RelationalExpressionType;

import java.util.List;

public class MergeExpression
    extends RelationalExpression
{
    public MergeExpression(int id, List<RelationalExpression> inputs)
    {
        super(id, inputs.get(0).getType(), inputs);
    }

    @Override
    public RelationalExpression copyWithInputs(int id, List<RelationalExpression> inputs)
    {
        return new MergeExpression(id, inputs);
    }

    @Override
    public String toStringTree(int indent)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(Utils.indent(indent) + "- merge(...):" + getType() + "\n")
                .append(Utils.indent(indent + 1) + "row type: " + getType() + "\n");

        for (RelationalExpression input : getInputs()) {
            builder.append(Utils.indent(indent + 1) + "inputs:" + "\n")
                    .append(input.toStringTree(indent + 2));
        }

        return builder.toString();
    }
}
