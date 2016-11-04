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
package com.facebook.presto.sql.optimizer.rule;

import com.facebook.presto.sql.optimizer.engine.Lookup;
import com.facebook.presto.sql.optimizer.engine.Rule;
import com.facebook.presto.sql.optimizer.tree.Aggregate;
import com.facebook.presto.sql.optimizer.tree.Expression;
import com.facebook.presto.sql.optimizer.tree.Filter;
import com.facebook.presto.sql.optimizer.tree.Intersect;
import com.facebook.presto.sql.optimizer.tree.Project;
import com.facebook.presto.sql.optimizer.tree.Union;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IntersectToUnion
        implements Rule
{
    @Override
    public Stream<Expression> apply(Expression expression, Lookup lookup)
    {
        return lookup.lookup(expression)
                .filter(Intersect.class::isInstance)
                .map(Intersect.class::cast)
                .map(this::process);
    }

    private Expression process(Intersect expression)
    {
        return new Filter("f",
                new Aggregate(
                        Aggregate.Type.SINGLE,
                        "a",
                        new Union(
                                expression.getArguments().stream()
                                        .map(e -> new Project("x", e))
                                        .collect(Collectors.toList()))));
    }
}
