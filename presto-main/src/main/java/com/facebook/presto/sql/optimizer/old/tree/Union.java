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

public class Union
        extends Expression
{
    public Union(List<Expression> arguments)
    {
        super(arguments);
    }

    @Override
    public String getName()
    {
        return "union";
    }

    public Union(Expression... arguments)
    {
        super(ImmutableList.copyOf(arguments));
    }

    @Override
    public Expression copyWithArguments(List<Expression> arguments)
    {
        return new Union(arguments);
    }

    @Override
    public String toString()
    {
        return String.format("(union %s)", getArguments());
    }

    @Override
    protected boolean shallowEquals(Expression other)
    {
        return other instanceof Union;
    }

    @Override
    protected int shallowHashCode()
    {
        return getClass().hashCode();
    }
}
