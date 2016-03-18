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

import com.google.common.base.Joiner;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Call
    extends Expression
{
    private final String name;

    public Call(String name, Expression... arguments)
    {
        super(Arrays.asList(arguments));
        this.name = name;
    }

    public Call(String name, List<Expression> arguments)
    {
        super(arguments);
        this.name = name;
    }

    @Override
    public boolean isPhysical()
    {
        return false;
    }

    @Override
    public boolean isLogical()
    {
        return true;
    }

    @Override
    public Expression copyWithArguments(List<Expression> arguments)
    {
        return new Call(name, arguments);
    }

    @Override
    public String toString()
    {
        if (getArguments().isEmpty()) {
            return name;
        }

        return String.format("(%s %s)", name, Joiner.on(" ").join(getArguments()));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Call call = (Call) o;
        return Objects.equals(name, call.name) && Objects.equals(getArguments(), call.getArguments());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, getArguments());
    }
}
