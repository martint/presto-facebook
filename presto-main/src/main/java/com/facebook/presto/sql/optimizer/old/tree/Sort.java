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
package com.facebook.presto.sql.optimizer.old.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

public class Sort
    extends Expression
{
    private final String criteria;

    public Sort(String criteria, Expression argument)
    {
        super(ImmutableList.of(argument));
        this.criteria = criteria;
    }

    public String getCriteria()
    {
        return criteria;
    }

    @Override
    public String getName()
    {
        return "sort";
    }

    @Override
    public Expression copyWithArguments(List<Expression> arguments)
    {
        checkArgument(arguments.size() == 1);
        return new Sort(criteria, arguments.get(0));
    }

    @Override
    public String toString()
    {
        return String.format("(sort %s %s)", criteria, getArguments().get(0));
    }

    @Override
    protected boolean shallowEquals(Expression other)
    {
        Sort that = (Sort) other;
        return Objects.equals(criteria, that.criteria);
    }

    @Override
    protected int shallowHashCode()
    {
        return criteria.hashCode();
    }
}
