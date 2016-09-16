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

public class Project
    extends Expression
{
    private final String expression;

    public Project(String expression, Expression argument)
    {
        super(ImmutableList.of(argument));
        this.expression = expression;
    }

    public String getExpression()
    {
        return expression;
    }

    @Override
    public String getName()
    {
        return "project";
    }

    @Override
    public Expression copyWithArguments(List<Expression> arguments)
    {
        checkArgument(arguments.size() == 1);
        return new Project(expression, arguments.get(0));
    }

    @Override
    public String toString()
    {
        return String.format("(project %s %s)", expression, getArguments().get(0));
    }

    @Override
    protected boolean shallowEquals(Expression other)
    {
        Project that = (Project) other;
        return Objects.equals(expression, that.expression);
    }

    @Override
    protected int shallowHashCode()
    {
        return expression.hashCode();
    }
}
