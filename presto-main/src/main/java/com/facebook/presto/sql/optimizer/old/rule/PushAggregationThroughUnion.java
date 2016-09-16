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
import com.facebook.presto.sql.optimizer.old.tree.Aggregate;
import com.facebook.presto.sql.optimizer.old.tree.Expression;
import com.facebook.presto.sql.optimizer.old.tree.Union;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PushAggregationThroughUnion
        implements Rule
{
    @Override
    public Stream<Expression> apply(Expression expression, Lookup lookup)
    {
        return lookup.lookup(expression)
                .filter(Aggregate.class::isInstance)
                .map(Aggregate.class::cast)
                .filter(e -> e.getType() == Aggregate.Type.SINGLE)
                .flatMap(parent -> lookup.lookup(parent.getArguments().get(0))
                        .filter(Union.class::isInstance)
                        .map(Union.class::cast)
                        .map(child -> process(parent, child)));
    }

    private Expression process(Aggregate parent, Union child)
    {
        return new Aggregate(
                Aggregate.Type.FINAL,
                parent.getFunction(),
                new Union(
                        child.getArguments().stream()
                                .map(grandChild -> new Aggregate(Aggregate.Type.PARTIAL, parent.getFunction(), grandChild))
                                .collect(Collectors.toList())));
    }
}
