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

import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.newplanner.RelationalExpressionType;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nullable;

import java.util.List;

import static com.facebook.presto.sql.relational.RowExpression.typeGetter;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;

public class AggregationExpression
        extends RelationalExpression
{
    // one filter field for each aggregate
    private final List<Signature> aggregates;
    private final List<Optional<Integer>> filterFields;
    private final List<List<Integer>> arguments;

    public AggregationExpression(int id, RelationalExpression input, RelationalExpressionType type, List<Signature> aggregates, List<Optional<Integer>> filterFields, List<List<Integer>> arguments)
    {
        super(id, type, ImmutableList.of(input));

        this.aggregates = aggregates;
        this.filterFields = filterFields;
        this.arguments = arguments;
    }

    public List<Optional<Integer>> getFilterFields()
    {
        return filterFields;
    }

    public List<Signature> getAggregates()
    {
        return aggregates;
    }

    public List<List<Integer>> getArguments()
    {
        return arguments;
    }

    @Override
    public String toStringTree(int indent)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(Utils.indent(indent) + "- aggregation" + "\n")
                .append(Utils.indent(indent + 1) + "row type: " + getType() + "\n")
                .append(Utils.indent(indent + 1) + "aggregates:" + "\n");


        for (int i = 0; i < aggregates.size(); i++) {
            builder.append(Utils.indent(indent + 2) + "function: " + aggregates.get(i) + "\n");
            builder.append(Utils.indent(indent + 2) + "arguments: " + Joiner.on(", ").join(IterableTransformer.on(arguments.get(i)).transform(new Function<Integer, String>() {
                @Override
                public String apply(Integer input)
                {
                    return "#" + input;
                }
            }).list()) + "\n");
            if (filterFields.get(i).isPresent()) {
                builder.append(Utils.indent(indent + 2) + "filter: #" + filterFields.get(i).toString() + "\n");
            }
        }

        builder.append(Utils.indent(indent + 1) + "input:" + "\n")
                .append(getInputs().get(0).toStringTree(indent + 2));

        return builder.toString();
    }
}
