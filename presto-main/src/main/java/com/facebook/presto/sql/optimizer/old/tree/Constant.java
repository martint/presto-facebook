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

public class Constant
        extends Expression
{
    private final Object value;

    public Constant(Object value)
    {
        super(ImmutableList.of());
        this.value = value;
    }

    @Override
    public String getName()
    {
        return value.toString();
    }

    @Override
    public Expression copyWithArguments(List<Expression> arguments)
    {
        return this;
    }

    @Override
    protected int shallowHashCode()
    {
        return value.hashCode();
    }

    @Override
    protected boolean shallowEquals(Expression other)
    {
        Constant that = (Constant) other;
        return Objects.equals(value, that.value);
    }
}
