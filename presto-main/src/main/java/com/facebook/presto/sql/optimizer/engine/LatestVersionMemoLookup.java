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
import com.facebook.presto.sql.optimizer.tree.Reference;
import com.google.common.primitives.Longs;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A lookup that avoids re-visiting the same entries (due to cycles in the memo)
 *
 * Expressions can only be looked up once.
 * TODO: maybe add the "stack" as an argument to the lookup method and check against
 * those entries to see if the there's a cycle instead of making this implementation
 * non-idempotent.
 */
class LatestVersionMemoLookup
        implements Lookup
{
    private final Set<Expression> visited = new HashSet<>();
    private final Memo memo;

    public LatestVersionMemoLookup(Memo memo, String group)
    {
        this.memo = memo;
        visited.add(new Reference(group));
    }

    @Override
    public Stream<Expression> lookup(Expression expression)
    {
        if (visited.contains(expression)) {
            return Stream.empty();
        }
        visited.add(expression);

        if (expression instanceof Reference) {
            return memo.getExpressions(((Reference) expression).getName()).stream()
                    .sorted((e1, e2) -> -Longs.compare(e1.getVersion(), e2.getVersion()))
                    .limit(1)
                    .map(VersionedItem::get);
        }
        return Stream.of(expression);
    }
}
