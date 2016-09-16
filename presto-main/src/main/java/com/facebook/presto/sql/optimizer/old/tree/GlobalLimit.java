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

public class GlobalLimit
        extends Expression
{
    private final long count;

    public GlobalLimit(long count, Expression argument)
    {
        super(ImmutableList.of(argument));
        this.count = count;
    }

    public long getCount()
    {
        return count;
    }

    @Override
    public String getName()
    {
        return "global-limit";
    }

    @Override
    public Expression copyWithArguments(List<Expression> arguments)
    {
        checkArgument(arguments.size() == 1);
        return new GlobalLimit(count, arguments.get(0));
    }

    @Override
    public String toString()
    {
        return String.format("(limit %s %s)", count, getArguments().get(0));
    }

    @Override
    protected boolean shallowEquals(Expression other)
    {
        GlobalLimit that = (GlobalLimit) other;
        return Objects.equals(count, that.count);
    }

    @Override
    protected int shallowHashCode()
    {
        return Long.hashCode(count);
    }
}
