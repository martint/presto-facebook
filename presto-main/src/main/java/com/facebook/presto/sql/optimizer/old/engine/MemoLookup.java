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
package com.facebook.presto.sql.optimizer.old.engine;

import com.facebook.presto.sql.optimizer.old.tree.Expression;
import com.facebook.presto.sql.optimizer.old.tree.Reference;
import com.google.common.collect.ImmutableSet;

import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A lookup that avoids re-visiting the same entries (due to cycles in the memo)
 *
 * Expressions can only be looked up once.
 * TODO: maybe add the "stack" as an argument to the lookup method and check against
 * those entries to see if the there's a cycle instead of making this implementation
 * non-idempotent.
 * TODO: or, add a push(group) method that gets us a new lookup that maintains the stack internally
 */
class MemoLookup
        implements Lookup
{
    private final Memo memo;
    private final Set<String> visited;
    private final Function<Stream<VersionedItem<Expression>>, Stream<VersionedItem<Expression>>> selector;

    public MemoLookup(Memo memo, String group)
    {
        this(memo, group, Function.identity());
    }

    public MemoLookup(Memo memo, String group, Function<Stream<VersionedItem<Expression>>, Stream<VersionedItem<Expression>>> selector)
    {
        this(memo, selector, ImmutableSet.of(group));
    }

    private MemoLookup(Memo memo, Function<Stream<VersionedItem<Expression>>, Stream<VersionedItem<Expression>>> selector, Set<String> visited)
    {
        this.memo = memo;
        this.visited = ImmutableSet.copyOf(visited);
        this.selector = selector;
    }

    @Override
    public Stream<Expression> lookup(Expression expression)
    {
        if (expression instanceof Reference) {
            String name = ((Reference) expression).getName();

            Stream<VersionedItem<Expression>> candidates = memo.getExpressions(name).stream()
                    .filter(e -> !e.get().getArguments().stream()
                            .map(Reference.class::cast)
                            .map(Reference::getName)
                            .anyMatch(visited::contains));

            return selector.apply(candidates).map(VersionedItem::get);
        }

        return Stream.of(expression);
    }

    public MemoLookup push(String group)
    {
        Set<String> visited = ImmutableSet.<String>builder()
                .addAll(this.visited)
                .add(group)
                .build();

        return new MemoLookup(memo, selector, visited);
    }
}
