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

import java.util.Objects;

public class Assignment
{
    private final String variable;
    private final Expression expression;

    public Assignment(String variable, Expression expression)
    {
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
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Assignment that = (Assignment) o;
        return Objects.equals(variable, that.variable) &&
                Objects.equals(expression, that.expression);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(variable, expression);
    }

    @Override
    public String toString()
    {
        return String.format("(%s %s)", variable, expression);
    }
}
