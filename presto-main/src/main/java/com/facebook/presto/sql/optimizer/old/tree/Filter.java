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

import static com.google.common.base.Preconditions.checkArgument;

public class Filter
    extends Expression
{
    public Filter(Expression input, Lambda criteria)
    {
        super(ImmutableList.of(input, criteria));
    }

    public Lambda getCriteria()
    {
        return (Lambda) getArguments().get(1);
    }

    @Override
    public String getName()
    {
        return "filter";
    }

    @Override
    public Expression copyWithArguments(List<Expression> arguments)
    {
        checkArgument(arguments.size() == 2);
        return new Filter(arguments.get(0), (Lambda) arguments.get(1));
    }

    @Override
    protected boolean shallowEquals(Expression other)
    {
        return other instanceof Filter;
    }

    @Override
    protected int shallowHashCode()
    {
        return getClass().hashCode();
    }
}
