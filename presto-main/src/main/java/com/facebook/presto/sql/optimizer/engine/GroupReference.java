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
package com.facebook.presto.sql.optimizer.engine;

import com.facebook.presto.sql.optimizer.tree.Expression;
import com.facebook.presto.sql.optimizer.tree.type.TypeStamp;

import java.util.Objects;

public class GroupReference
    extends Expression
{
    private final long id;

    public static GroupReference group(TypeStamp type, long id)
    {
        return new GroupReference(type, id);
    }

    public GroupReference(TypeStamp type, long id)
    {
        super(type);
        this.id = id;
    }

    public long getId()
    {
        return id;
    }

    @Override
    public Object terms()
    {
        return "$" + id;
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
        GroupReference that = (GroupReference) o;
        return id == that.id;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id);
    }
}
