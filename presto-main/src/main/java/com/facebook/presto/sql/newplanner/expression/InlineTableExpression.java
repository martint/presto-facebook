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
import com.facebook.presto.sql.relational.ConstantExpression;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class InlineTableExpression
    extends RelationalExpression
{
    private final List<List<ConstantExpression>> rows;

    public InlineTableExpression(int id, List<Type> rowType, List<List<ConstantExpression>> rows)
    {
        super(id, new RelationalExpressionType(rowType), ImmutableList.<RelationalExpression>of());

        checkNotNull(rows, "rows is null");
        this.rows = rows;
    }

    public List<List<ConstantExpression>> getRows()
    {
        return rows;
    }

    @Override
    public String toStringTree(int indent)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(Utils.indent(indent) + "- inline table" + "\n")
                .append(Utils.indent(indent + 1) + "row type: " + getType() + "\n")
                .append(Utils.indent(indent + 1) + "rows:" + "\n");

        for (List<ConstantExpression> row : rows) {
            builder.append(Utils.indent(indent + 2) + "(" + Joiner.on(", ").join(row) + ")" + "\n");
        }

        return builder.toString();
    }
}
