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

import com.facebook.presto.sql.optimizer.tree.type.FunctionTypeStamp;
import com.facebook.presto.sql.optimizer.tree.type.TypeStamp;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.optimizer.tree.Expressions.variable;
import static java.util.Objects.requireNonNull;

public class Apply
        extends Expression
{
    private final Expression target;

    // TODO: should arguments be a single value? (and encode multiple args as row values)
    private final List<Expression> arguments;

    protected Apply(TypeStamp type, String name, List<Expression> arguments)
    {
        this(type, variable(new FunctionTypeStamp(), name), arguments); // TODO: type for function name (e.g. signature)
    }

    public Apply(TypeStamp type, Expression target, List<Expression> arguments)
    {
        super(type);

        requireNonNull(target, "target is null");
        requireNonNull(arguments, "arguments is null");

        this.target = target;
        this.arguments = arguments;
    }

    public Expression getTarget()
    {
        return target;
    }

    public List<Expression> getArguments()
    {
        return arguments;
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
        Apply apply = (Apply) o;
        return Objects.equals(target, apply.target) && Objects.equals(arguments, apply.arguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(target, arguments);
    }

    @Override
    public List<Object> terms()
    {
        return ImmutableList.builder()
                .add(target.terms())
                .addAll(arguments.stream().map(Expression::terms).collect(Collectors.toList()))
                .build();
    }
}
