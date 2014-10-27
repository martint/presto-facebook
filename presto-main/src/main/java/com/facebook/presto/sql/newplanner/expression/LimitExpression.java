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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public final class LimitExpression
        extends RelationalExpression
{
    private final long limit;

    public LimitExpression(int id, RelationalExpression expression, long limit)
    {
        super(id, expression.getType(), ImmutableList.of(expression));
        this.limit = limit;
    }

    public long getLimit()
    {
        return limit;
    }

    @Override
    public RelationalExpression copyWithInputs(int id, List<RelationalExpression> inputs)
    {
        checkArgument(inputs.size() == 1, "Expected 1 input");
        return new LimitExpression(id, Iterables.getOnlyElement(inputs), limit);
    }

    @Override
    public String toStringTree(int indent)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(Utils.indent(indent) + "- limit(...):" + getType() + "\n")
                .append(Utils.indent(indent + 1) + "count:" + limit + "\n");

        builder.append(Utils.indent(indent + 1) + "input:" + "\n")
                .append(getInputs().get(0).toStringTree(indent + 2));

        return builder.toString();
    }
}
