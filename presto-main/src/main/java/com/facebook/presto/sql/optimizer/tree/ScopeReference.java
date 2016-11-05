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

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A reference to a lambda binding variable encoded as a De Bruijn index.
 * <p>
 * The level represents the number of nested scope levels the reference
 * points to. Thus, level = 0 is a reference to the immediately enclosing
 * lambda.
 */
public final class ScopeReference
        extends Expression
        implements Atom
{
    private final int index;

    ScopeReference(TypeStamp type, int index)
    {
        super(type);
        checkArgument(index >= 0, "level must be >= 0");
        this.index = index;
    }

    public int getIndex()
    {
        return index;
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
        ScopeReference that = (ScopeReference) o;
        return index == that.index;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(index);
    }

    @Override
    public Object terms()
    {
        return "%" + index;
    }
}
