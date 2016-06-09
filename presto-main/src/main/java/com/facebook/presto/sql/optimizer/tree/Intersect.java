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

public class Intersect
        extends Expression<Intersect>
{
    public Intersect(List<Expression<?>> arguments)
    {
        super(arguments);
    }
    public Intersect(Expression<?>... arguments)
    {
        super(ImmutableList.copyOf(arguments));
    }

    @Override
    public boolean isPhysical()
    {
        return true;
    }

    @Override
    public boolean isLogical()
    {
        return true;
    }

    @Override
    public Expression<?> copyWithArguments(List<Expression<?>> arguments)
    {
        return new Intersect(arguments);
    }

    @Override
    public String toString()
    {
        return String.format("(intersect %s)", getArguments());
    }

    @Override
    protected boolean shallowEquals(Intersect other)
    {
        return true;
    }

    @Override
    protected int shallowHashCode()
    {
        return getClass().hashCode();
    }
}
