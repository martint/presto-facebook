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

public class Join
        extends Expression
{
    public enum Type
    {
        LEFT,
        RIGHT,
        FULL,
        INNER
    }

    private final Type type;
    private final String criteria;

    public Join(Type type, String criteria, Expression left, Expression right)
    {
        super(ImmutableList.of(left, right));
        this.type = type;
        this.criteria = criteria;
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
        return true;
    }

    @Override
    public Expression copyWithArguments(List<Expression> arguments)
    {
        return new Join(type, criteria, arguments.get(0), arguments.get(1));
    }

    @Override
    public String toString()
    {
        return String.format("(join %s %s %s)", type, criteria, getArguments());
    }

    @Override
    protected boolean shallowEquals(Expression other)
    {
        Join that = (Join) other;
        return Objects.equals(type, that.type) &&
                Objects.equals(criteria, that.criteria);
    }

    @Override
    protected int shallowHashCode()
    {
        return Objects.hash(type, criteria);
    }
}
