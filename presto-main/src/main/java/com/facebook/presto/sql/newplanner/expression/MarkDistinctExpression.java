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

import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.sql.newplanner.RelationalExpressionType;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;

// markDistinct(Row<A, B, ...>, (r) -> (...)):Row<A, B, ..., BOOLEAN>
public final class MarkDistinctExpression
        extends RelationalExpression
{
    private final List<Integer> distinctFields;

    public MarkDistinctExpression(int id, RelationalExpression input, List<Integer> distinctFields)
    {
        super(id, getOutputType(input), ImmutableList.of(input));
        this.distinctFields = distinctFields;
    }

    public List<Integer> getDistinctFields()
    {
        return distinctFields;
    }

    @Override
    public String toStringTree(int indent)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(Utils.indent(indent) + "- markDistinct" + "\n")
                .append(Utils.indent(indent + 1) + "row type: " + getType() + "\n")
                .append(Utils.indent(indent + 2) + "fields: " + Joiner.on(", ").join(IterableTransformer.on(distinctFields).transform(new Function<Integer, String>()
                {
                    @Override
                    public String apply(Integer input)
                    {
                        return "#" + input;
                    }
                }).list()) + "\n")
                .append(Utils.indent(indent + 1) + "input:" + "\n")
                .append(getInputs().get(0).toStringTree(indent + 2));

        return builder.toString();
    }

    private static RelationalExpressionType getOutputType(RelationalExpression input)
    {
        return new RelationalExpressionType(ImmutableList.copyOf(Iterables.concat(input.getType().getRowType(), ImmutableList.of(BooleanType.BOOLEAN))));
    }
}
