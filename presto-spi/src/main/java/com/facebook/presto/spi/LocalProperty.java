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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public interface LocalProperty<E>
{
    <T> Optional<LocalProperty<T>> translate(Function<E, Optional<T>> translator);

    boolean canCombine(LocalProperty<E> actual);

    default Optional<LocalProperty<E>> withConstants(Set<E> constants)
    {
        Set<E> set = new HashSet<>(getColumns());
        set.removeAll(constants);

        if (set.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(constrain(set));
    }

    /**
     * Return a new instance with the give (reduced) set of columns
     */
    default LocalProperty<E> constrain(Set<E> columns)
    {
        if (!columns.equals(getColumns())) {
            throw new IllegalArgumentException(String.format("Cannot constrain %s with %s", this, columns));
        }
        return this;
    }

    Set<E> getColumns();

    /**
     * Attempt to match the desired properties to a sequence of known properties.
     *
     * Returns a list of the same length as the original. Entries are:
     *    - Optional.empty(): the property was satisfied completely
     *    - non-empty: the (simplified) property that was not satisfied
     */
    static <T> List<Optional<LocalProperty<T>>> match(List<LocalProperty<T>> actuals, List<LocalProperty<T>> desired)
    {
        List<Optional<LocalProperty<T>>> result = new ArrayList<>();

        Iterator<LocalProperty<T>> actualIterator = actuals.iterator();
        Iterator<LocalProperty<T>> desiredIterator = desired.iterator();

        Set<T> constants = new HashSet<>();
        Optional<LocalProperty<T>> wanted = Optional.empty();
        while (true) {
            Optional<LocalProperty<T>> actual = pickNext(actualIterator, constants);

            if (!actual.isPresent()) {
                // we're done
                break;
            }

            if (!wanted.isPresent() && desiredIterator.hasNext()) {
                wanted = Optional.of(desiredIterator.next());
            }

            if (!wanted.isPresent()) {
                break;
            }

            // simplify the current wanted property based on known constants
            wanted = wanted.get().withConstants(constants);
            if (!wanted.isPresent()) {
                result.add(Optional.empty());
                continue;
            }

            if (!wanted.get().canCombine(actual.get())) {
                break;
            }

            Set<T> newConstants = actual.get().getColumns();
            constants.addAll(newConstants);

            // simplify the current wanted property based on the constants derived
            // as a result of applying the current actual property
            wanted = wanted.get().withConstants(newConstants);
            if (!wanted.isPresent()) {
                result.add(Optional.empty());
            }
        }

        // simplify the properties we couldn't match based on the constants
        // we just derived and the ones that each of the properties implies
        if (wanted.isPresent()) {
            result.add(wanted.get().withConstants(constants));
            constants.addAll(wanted.get().getColumns());
        }

        while (desiredIterator.hasNext()) {
            LocalProperty<T> next = desiredIterator.next();
            result.add(next.withConstants(constants));
            constants.addAll(next.getColumns());
        }

        return result;
    }

    static <T> Optional<LocalProperty<T>> pickNext(Iterator<LocalProperty<T>> iterator, Set<T> knownConstants)
    {
        while (iterator.hasNext()) {
            Optional<LocalProperty<T>> actual = iterator.next().withConstants(knownConstants);

            if (actual.isPresent()) {
                return actual;
            }
        }

        return Optional.empty();
    }
}
