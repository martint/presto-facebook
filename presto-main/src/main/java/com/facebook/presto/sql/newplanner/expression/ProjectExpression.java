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
import com.facebook.presto.sql.relational.RowExpression;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

public final class ProjectExpression
        extends RelationalExpression
{
    private final List<RowExpression> projections;

    public ProjectExpression(int id, RelationalExpression input, List<RowExpression> projections)
    {
        super(id, new RelationalExpressionType(Lists.transform(projections, typeGetter())), ImmutableList.of(input));
        this.projections = projections;
    }

    public List<RowExpression> getProjections()
    {
        return projections;
    }

    @Override
    public String toStringTree(int indent)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(Utils.indent(indent) + "- project" + "\n")
                .append(Utils.indent(indent + 1) + "row type: " + getType() + "\n")
                .append(Utils.indent(indent + 1) + "projections:" + "\n");

        for (RowExpression projection : projections) {
            builder.append(Utils.indent(indent + 2) + projection + "\n");
        }

        builder.append(Utils.indent(indent + 1) + "input:" + "\n")
                .append(getInputs().get(0).toStringTree(indent + 2));

        return builder.toString();
    }

    private static Function<? super RowExpression, Type> typeGetter()
    {
        return new Function<RowExpression, Type>()
        {
            @Override
            public Type apply(RowExpression input)
            {
                return input.getType();
            }
        };
    }
}
