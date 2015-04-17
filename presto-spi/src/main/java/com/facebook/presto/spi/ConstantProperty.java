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
package com.facebook.presto.spi;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public final class ConstantProperty<E>
        implements LocalProperty<E>
{
    private final E column;

    public ConstantProperty(E column)
    {
        this.column = column;
    }

    public E getColumn()
    {
        return column;
    }

    public Set<E> getColumns()
    {
        HashSet<E> result = new HashSet<>();
        result.add(column);

        return result;
    }

    @Override
    public <T> Optional<LocalProperty<T>> translate(Function<E, Optional<T>> translator)
    {
        Optional<T> translated = translator.apply(column);

        if (translated.isPresent()) {
            return Optional.of(new ConstantProperty<>(translated.get()));
        }

        return Optional.empty();
    }

    @Override
    public boolean canCombine(LocalProperty<E> known)
    {
        return (known instanceof ConstantProperty && known.getColumns().equals(getColumns()));
    }

    @Override
    public String toString()
    {
        return "C(" + column + ")";
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
        ConstantProperty<?> that = (ConstantProperty<?>) o;
        return Objects.equals(column, that.column);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column);
    }
}
