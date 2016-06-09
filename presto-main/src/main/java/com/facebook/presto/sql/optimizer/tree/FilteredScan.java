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
package com.facebook.presto.sql.optimizer.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

public class FilteredScan
        extends Expression
{
    private final String criteria;
    private final String table;

    public FilteredScan(String table, String criteria)
    {
        super(ImmutableList.of());
        this.table = table;
        this.criteria = criteria;
    }

    public String getTable()
    {
        return table;
    }

    public String getCriteria()
    {
        return criteria;
    }

    @Override
    public boolean isPhysical()
    {
        return true;
    }

    @Override
    public boolean isLogical()
    {
        return false;
    }

    @Override
    public Expression copyWithArguments(List<Expression> arguments)
    {
        checkArgument(arguments.isEmpty());
        return this;
    }

    @Override
    public String toString()
    {
        return String.format("(filtered-scan '%s' %s)", table, criteria);
    }

    protected boolean shallowEquals(Expression other)
    {
        FilteredScan that = (FilteredScan) other;
        return Objects.equals(criteria, that.criteria) &&
                Objects.equals(table, that.table);
    }

    @Override
    protected int shallowHashCode()
    {
        return Objects.hash(criteria, table);
    }
}
