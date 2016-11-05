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

import com.facebook.presto.sql.optimizer.tree.type.LambdaTypeStamp;
import com.facebook.presto.sql.optimizer.tree.type.TypeStamp;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.sql.optimizer.utils.CollectionConstructors.list;

public final class Expressions
{
    private Expressions()
    {
    }

    public static Apply apply(TypeStamp type, Expression target, Expression... arguments)
    {
        return new Apply(type, target, list(arguments));
    }

    public static Apply call(TypeStamp type, String name, Expression... arguments)
    {
        return new Apply(type, name, ImmutableList.copyOf(arguments));
    }

    public static Apply call(TypeStamp type, String name, List<Expression> arguments)
    {
        return new Apply(type, name, ImmutableList.copyOf(arguments));
    }

    public static Lambda lambda(LambdaTypeStamp argumentType, Expression body)
    {
        return new Lambda(argumentType, body);
    }

    public static Reference variable(TypeStamp type, String name)
    {
        return new Reference(type, name);
    }

    public static ScopeReference localReference(TypeStamp type)
    {
        return reference(type, 0);
    }

    public static ScopeReference reference(TypeStamp type, int index)
    {
        return new ScopeReference(type, index);
    }

    public static Value value(TypeStamp type, Object value)
    {
        return new Value(type, value);
    }

    public static Let let(List<Assignment> assignments, Expression body)
    {
        return new Let(assignments, body);
    }

    public static Apply fieldDereference(TypeStamp type, Expression expression, String field)
    {
        return call(type, "field-deref", expression, value(type, field));
    }

    public static Apply fieldDereference(TypeStamp type, Expression expression, int field)
    {
        return call(type, "field-deref", expression, value(type, field));
    }
}
