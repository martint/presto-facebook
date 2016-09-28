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
        if (!(expression instanceof Apply)) {
            return Stream.empty();
        }

        Apply apply = (Apply) expression;
        Expression target = lookup.resolve(apply.getTarget());
        if (!(target instanceof Lambda)) {
            return Stream.empty();
        }

        Lambda lambda = (Lambda) target;

        return Stream.of(substitute(lookup.resolve(lambda.getBody()), apply.getArguments().get(0), lookup));
    }

    // substitute all occurrences of %0 with argument
    private Expression substitute(Expression expression, Expression argument, Lookup lookup)
    {
        if (expression instanceof ScopeReference && ((ScopeReference) expression).getIndex() == 0) {
            return argument;
        }
        else if (expression instanceof Apply) {
            // TODO substitute in target
            Apply apply = (Apply) expression;
            List<Expression> newArguments = apply.getArguments().stream()
                    .map(e -> substitute(e, argument, lookup))
                    .collect(toImmutableList());

            return new Apply(substitute(lookup.resolve(apply.getTarget()), argument, lookup), newArguments);
        }

        return expression;
    }
}
