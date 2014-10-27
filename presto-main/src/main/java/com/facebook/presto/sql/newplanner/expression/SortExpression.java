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

import com.facebook.presto.spi.block.SortOrder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public final class SortExpression
        extends RelationalExpression
{
    private final List<Integer> sortFields;
    private final List<SortOrder> sortOrders;

    public SortExpression(int id, RelationalExpression input, List<Integer> sortFields, List<SortOrder> sortOrders)
    {
        super(id, input.getType(), ImmutableList.of(input));
        this.sortFields = ImmutableList.copyOf(sortFields);
        this.sortOrders = ImmutableList.copyOf(sortOrders);
    }

    public List<Integer> getSortFields()
    {
        return sortFields;
    }

    public List<SortOrder> getSortOrders()
    {
        return sortOrders;
    }

    @Override
    public RelationalExpression copyWithInputs(int id, List<RelationalExpression> inputs)
    {
        checkArgument(inputs.size() == 1, "Expected 1 input");
        return new SortExpression(id, Iterables.getOnlyElement(inputs), sortFields, sortOrders);
    }

    @Override
    public String toStringTree(int indent)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(Utils.indent(indent) + "- sort(...):" + getType() + "\n")
                .append(Utils.indent(indent + 1) + "sort fields: " + "\n");

        for (int i = 0; i < sortFields.size(); i++) {
            builder.append(Utils.indent(indent + 2) + "#" + sortFields.get(i) + " " + sortOrders.get(i) + "\n");
        }

        builder.append(Utils.indent(indent + 1) + "input:" + "\n")
                .append(getInputs().get(0).toStringTree(indent + 2));

        return builder.toString();
    }
}
