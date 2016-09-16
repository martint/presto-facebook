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

public class Call
    extends Expression
{
    private String function;

    public Call(String function, Expression... arguments)
    {
        this(function, ImmutableList.copyOf(arguments));
    }

    public Call(String function, List<Expression> arguments)
    {
        super(arguments);
        this.function = function;
    }

    public String getFunction()
    {
        return function;
    }

    @Override
    public String getName()
    {
        return "call";
    }

    @Override
    public Expression copyWithArguments(List<Expression> arguments)
    {
        return new Call(function, arguments);
    }

    @Override
    public String toString()
    {
        return String.format("(call '%s' %s)", function, getArguments());
    }

    @Override
    protected boolean shallowEquals(Expression other)
    {
        Call that = (Call) other;
        return Objects.equals(function, that.function);
    }

    @Override
    protected int shallowHashCode()
    {
        return function.hashCode();
    }
}
