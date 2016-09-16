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

import static com.facebook.presto.sql.optimizer.utils.CollectionConstructors.list;

public class Apply
    extends Expression
{
    public Apply(Expression lambda, Expression argument)
    {
        super(list(lambda, argument));
    }

    public Expression getLambda()
    {
        return getArguments().get(0);
    }

    public Expression getInput()
    {
        return getArguments().get(1);
    }

    @Override
    public String getName()
    {
        return "apply";
    }

    @Override
    public Expression copyWithArguments(List<Expression> arguments)
    {
        return new Apply(arguments.get(0), arguments.get(1));
    }

    @Override
    protected int shallowHashCode()
    {
        return getClass().hashCode();
    }

    @Override
    protected boolean shallowEquals(Expression other)
    {
        return other instanceof Apply;
    }

    @Override
    public String toString()
    {
        return String.format("(apply %s %s)", getArguments().get(0), getArguments().get(1));
    }
}
