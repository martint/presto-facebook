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

import static com.facebook.presto.sql.optimizer.utils.CollectionConstructors.list;

public final class Expressions
{
    private Expressions()
    {
    }

    public static Apply apply(Expression target, Expression... arguments)
    {
        return new Apply(target, list(arguments));
    }

    public static Apply call(String name, Expression... arguments)
    {
        return new Apply(name, ImmutableList.copyOf(arguments));
    }

    public static Apply call(String name, List<Expression> arguments)
    {
        return new Apply(name, ImmutableList.copyOf(arguments));
    }

    public static Lambda lambda(Expression body)
    {
        return new Lambda(body);
    }

    public static Reference variable(String name)
    {
        return new Reference(name);
    }

    public static ScopeReference localReference()
    {
        return reference(0);
    }

    public static ScopeReference reference(int index)
    {
        return new ScopeReference(index);
    }

    public static Value value(Object value)
    {
        return new Value(value);
    }

    public static Let let(List<Assignment> assignments, Expression body)
    {
        return new Let(assignments, body);
    }

    public static Apply fieldDereference(Expression expression, String field)
    {
        return call("field-deref", expression, value(field));
    }

    public static Apply fieldDereference(Expression expression, int field)
    {
        return call("field-deref", expression, value(field));
    }
}
