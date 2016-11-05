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

import com.facebook.presto.sql.optimizer.tree.type.TypeStamp;
import com.facebook.presto.sql.optimizer.utils.ListFormatter;

import static java.util.Objects.requireNonNull;

public abstract class Expression
{
    private final TypeStamp type;

    public Expression(TypeStamp type)
    {
        this.type = requireNonNull(type, "type is null");
    }

    public TypeStamp type()
    {
        return type;
    }

    public abstract int hashCode();
    public abstract boolean equals(Object other);

    public abstract Object terms();

    @Override
    public String toString()
    {
        return ListFormatter.format(terms());
    }
}
