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

import java.util.List;
import java.util.Objects;

import static com.facebook.presto.sql.optimizer.utils.CollectionConstructors.list;

public class Lambda
    extends Expression
{
    private final String variable;
    private final Expression expression;

    public Lambda(String variable, Expression expression)
    {
        super(list());
        this.expression = expression;
        this.variable = variable;
    }

    public Expression getExpression()
    {
        return expression;
    }

    public String getVariable()
    {
        return variable;
    }

    @Override
    public String toString()
    {
        return String.format("(\\%s %s)", variable, expression);
    }

    @Override
    public String getName()
    {
        return "lambda";
    }

    @Override
    public Expression copyWithArguments(List<Expression> arguments)
    {
        return this;
    }

    @Override
    protected int shallowHashCode()
    {
        return Objects.hash(variable, expression);
    }

    @Override
    protected boolean shallowEquals(Expression other)
    {
        Lambda that = (Lambda) other;

        return Objects.equals(variable, that.variable) &&
                Objects.equals(expression, that.variable);
    }
}
