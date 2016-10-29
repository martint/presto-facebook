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
import com.facebook.presto.sql.optimizer.tree.Apply;
import com.facebook.presto.sql.optimizer.tree.Expression;
import com.facebook.presto.sql.optimizer.tree.Lambda;

import java.util.stream.Stream;

import static com.facebook.presto.sql.optimizer.engine.Patterns.isCall;
import static com.facebook.presto.sql.optimizer.tree.Expressions.value;
import static com.facebook.presto.sql.optimizer.utils.CollectionConstructors.list;

public class RemoveRedundantFilter
        implements Rule
{
    @Override
    public Stream<Expression> transform(Expression expression, Lookup lookup)
    {
        if (!isCall(expression, "logical-filter", lookup)) {
            return Stream.empty();
        }

        Apply filter = (Apply) expression;
        Lambda lambda = (Lambda) lookup.resolve(filter.getArguments().get(1));
        Expression body = lookup.resolve(lambda.getBody());

        if (body.equals(value(null, true))) { // TODO: type
            return Stream.of(filter.getArguments().get(1));
        }

        if (body.equals(value(null, false))) { // TODO: type
            return Stream.of(value(null, list())); // TODO: type
        }

        return Stream.empty();
    }
}
