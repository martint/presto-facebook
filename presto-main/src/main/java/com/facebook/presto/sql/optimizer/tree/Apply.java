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

import java.util.List;
import java.util.Objects;

import static com.facebook.presto.sql.optimizer.engine.CollectionConstructors.list;

public class Apply
    extends Expression
{
    private final Lambda lambda;

    public Apply(Lambda lambda, Expression argument)
    {
        super(list(argument));
        this.lambda = lambda;
    }

    public Lambda getLambda()
    {
        return lambda;
    }

    @Override
    public Expression copyWithArguments(List<Expression> arguments)
    {
        return new Apply(lambda, arguments.get(0));
    }

    @Override
    protected int shallowHashCode()
    {
        return lambda.hashCode();
    }

    @Override
    protected boolean shallowEquals(Expression other)
    {
        Apply that = (Apply) other;
        return Objects.equals(lambda, that.lambda);
    }

    @Override
    public String toString()
    {
        return String.format("(apply %s %s)", lambda, getArguments().get(0));
    }
}
