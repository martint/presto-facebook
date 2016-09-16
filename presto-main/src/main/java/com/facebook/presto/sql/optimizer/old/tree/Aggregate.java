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

public class Aggregate
        extends Expression
{
    public enum Type
    {
        FINAL,
        PARTIAL,
        SINGLE
    }

    private final String function;
    private final Type type;

    public Aggregate(Type type, String function, Expression argument)
    {
        super(ImmutableList.of(argument));
        this.type = type;
        this.function = function;
    }

    public Type getType()
    {
        return type;
    }

    public String getFunction()
    {
        return function;
    }

    @Override
    public String getName()
    {
        return "aggregate";
    }

    @Override
    public Expression copyWithArguments(List<Expression> arguments)
    {
        checkArgument(arguments.size() == 1);
        return new Aggregate(type, function, arguments.get(0));
    }

    @Override
    public String toString()
    {
        return String.format("(aggregate %s %s %s)", type, function, getArguments().get(0));
    }

    @Override
    protected boolean shallowEquals(Expression other)
    {
        Aggregate that = (Aggregate) other;
        return Objects.equals(type, that.type) &&
                Objects.equals(function, that.function);
    }

    @Override
    protected int shallowHashCode()
    {
        return Objects.hash(type, function);
    }
}
