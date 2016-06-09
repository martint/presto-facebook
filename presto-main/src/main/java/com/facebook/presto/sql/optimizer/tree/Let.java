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
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

public class Let
    extends Expression
{
    private final Map<String, Expression> assignments;
    private final Expression expression;

    public Let(Map<String, Expression> assignments, Expression expression)
    {
        super(ImmutableList.of());
        this.assignments = assignments;
        this.expression = expression;
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
        checkArgument(arguments.isEmpty());
        return this;
    }

    public Map<String, Expression> getAssignments()
    {
        return assignments;
    }

    public Expression getExpression()
    {
        return expression;
    }

    @Override
    public String toString()
    {
        return String.format("(let (%s) %s)", Joiner.on(" ").withKeyValueSeparator("=").join(assignments), expression);
    }

    @Override
    protected boolean shallowEquals(Expression other)
    {
        Let that = (Let) other;
        return Objects.equals(assignments, that.assignments) &&
                Objects.equals(expression, that.expression);    }

    @Override
    protected int shallowHashCode()
    {
        return Objects.hash(assignments, expression);
    }
}
