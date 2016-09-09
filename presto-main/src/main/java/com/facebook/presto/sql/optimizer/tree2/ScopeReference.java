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
package com.facebook.presto.sql.optimizer.tree2;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

public final class ScopeReference
        extends Expression
{
    private final int level;

    public static ScopeReference reference(int level)
    {
        return new ScopeReference(level);
    }

    public ScopeReference(int level)
    {
        checkArgument(level >= 0, "level must be >= 0");
        this.level = level;
    }

    public int getLevel()
    {
        return level;
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
        return level == that.level;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(level);
    }

    @Override
    public Object terms()
    {
        return "%" + level;
    }
}
