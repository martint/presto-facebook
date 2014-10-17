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

import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.newplanner.RelationalExpressionType;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class TableExpression
    extends RelationalExpression
{
    private final TableHandle table;
    private final List<ColumnHandle> columns;

    public TableExpression(int id, TableHandle table, List<ColumnHandle> columns, RelationalExpressionType type)
    {
        super(id, type, ImmutableList.<RelationalExpression>of());

        checkNotNull(table, "table is null");
        checkNotNull(columns, "columns is null");

        this.table = table;
        this.columns = ImmutableList.copyOf(columns);
    }

    public TableHandle getTable()
    {
        return table;
    }

    public List<ColumnHandle> getColumns()
    {
        return columns;
    }

    @Override
    public String toStringTree(int indent)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(Utils.indent(indent) + "- table(" + table + ")" + "\n");
        builder.append(Utils.indent(indent + 1) + "row type: " + getType() + "\n");
        builder.append(Utils.indent(indent + 1) + "columns:" + "\n");
        for (ColumnHandle column : columns) {
            builder.append(Utils.indent(indent + 2) + column + "\n");
        }
        return builder.toString();
    }
}
