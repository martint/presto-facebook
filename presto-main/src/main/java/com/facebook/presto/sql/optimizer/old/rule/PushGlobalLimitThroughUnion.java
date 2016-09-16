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
package com.facebook.presto.sql.optimizer.old.rule;

import com.facebook.presto.sql.optimizer.old.engine.Lookup;
import com.facebook.presto.sql.optimizer.old.engine.Rule;
import com.facebook.presto.sql.optimizer.old.tree.Expression;
import com.facebook.presto.sql.optimizer.old.tree.GlobalLimit;
import com.facebook.presto.sql.optimizer.old.tree.LocalLimit;
import com.facebook.presto.sql.optimizer.old.tree.Union;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class PushGlobalLimitThroughUnion
        implements Rule
{
    @Override
    public Stream<Expression> apply(Expression expression, Lookup lookup)
    {
        return lookup.lookup(expression)
                .filter(GlobalLimit.class::isInstance)
                .map(GlobalLimit.class::cast)
                .flatMap(parent -> lookup.lookup(parent.getArguments().get(0))
                        .filter(Union.class::isInstance)
                        .map(Union.class::cast)
                        .map(child -> process(parent, child, lookup))
                        .filter(Optional::isPresent)
                        .map(Optional::get));
    }

    private Optional<Expression> process(GlobalLimit parent, Union child, Lookup lookup)
    {
        List<Expression> children = new ArrayList<>();

        boolean success = false;
        for (Expression grandChild : child.getArguments()) {
            boolean hasLimit = lookup.lookup(grandChild)
                    .filter(LocalLimit.class::isInstance)
                    .map(LocalLimit.class::cast)
                    .anyMatch(e -> e.getCount() == parent.getCount());

            success = success || !hasLimit;

            if (hasLimit) {
                children.add(grandChild);
            }
            else {
                children.add(new LocalLimit(parent.getCount(), grandChild));
            }
        }

        if (success) {
            return Optional.of(
                    new GlobalLimit(
                            parent.getCount(),
                            new Union(children)));
        }

        return Optional.empty();
    }
}
