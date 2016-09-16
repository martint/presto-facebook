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

public class TopN
        extends Expression
{
    public enum Type {
        GLOBAL,
        LOCAL
    }

    private final Type type;
    private final long count;
    private final String criteria;

    public TopN(Type type, long count, String criteria, Expression argument)
    {
        super(ImmutableList.of(argument));
        this.type = type;
        this.count = count;
        this.criteria = criteria;
    }

    public Type getType()
    {
        return type;
    }

    public long getCount()
    {
        return count;
    }

    public String getCriteria()
    {
        return criteria;
    }

    @Override
    public String getName()
    {
        return "top-n";
    }

    @Override
    public Expression copyWithArguments(List<Expression> arguments)
    {
        checkArgument(arguments.size() == 1);
        return new TopN(type, count, criteria, arguments.get(0));
    }

    @Override
    public String toString()
    {
        return String.format("(top-n %s %s %s %s)", type, count, criteria, getArguments().get(0));
    }

    @Override
    protected boolean shallowEquals(Expression other)
    {
        TopN that = (TopN) other;
        return Objects.equals(type, that.type) &&
                Objects.equals(count, that.count) &&
                Objects.equals(criteria, that.criteria);
    }

    @Override
    protected int shallowHashCode()
    {
        return Objects.hash(type, count, criteria);
    }
}
