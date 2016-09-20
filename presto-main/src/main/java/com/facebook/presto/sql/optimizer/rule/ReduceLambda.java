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
import com.facebook.presto.sql.optimizer.tree.ScopeReference;

import java.util.List;
import java.util.stream.Stream;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;

public class ReduceLambda
    implements Rule
{
    @Override
    public Stream<Expression> apply(Expression expression, Lookup lookup)
    {
        return lookup.resolve(expression)
                .filter(Apply.class::isInstance)
                .map(Apply.class::cast)
                .flatMap(apply ->
                        lookup.resolve(apply.getTarget())
                            .filter(Lambda.class::isInstance)
                            .map(Lambda.class::cast)
                            .flatMap(lambda ->
                                    lookup.resolve(apply.getArguments().get(0))
                                        .map(argument -> substitute(lambda.getBody(), argument, lookup))));
    }

    // substitute all occurrences of %0 with argument
    private Expression substitute(Expression expression, Expression argument, Lookup lookup)
    {
        Expression resolved = lookup.first(expression);

        if (resolved instanceof ScopeReference && ((ScopeReference) resolved).getIndex() == 0) {
            return argument;
        }
        else if (resolved instanceof Apply) {
            // TODO substitute in target
            Apply apply = (Apply) resolved;
            List<Expression> newArguments = apply.getArguments().stream()
                    .map(e -> substitute(e, argument, lookup))
                    .collect(toImmutableList());

            return new Apply(substitute(apply.getTarget(), argument, lookup), newArguments);
        }

        return expression;
    }
}
