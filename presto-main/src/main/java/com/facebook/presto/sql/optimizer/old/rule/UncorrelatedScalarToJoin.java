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
import com.facebook.presto.sql.optimizer.old.tree.Apply;
import com.facebook.presto.sql.optimizer.old.tree.CrossJoin;
import com.facebook.presto.sql.optimizer.old.tree.EnforceScalar;
import com.facebook.presto.sql.optimizer.old.tree.Expression;
import com.facebook.presto.sql.optimizer.old.tree.Lambda;
import com.facebook.presto.sql.optimizer.old.tree.Reference;

import java.util.stream.Stream;

public class UncorrelatedScalarToJoin
        implements Rule
{
    @Override
    public Stream<Expression> apply(Expression expression, Lookup lookup)
    {
        return lookup.lookup(expression)
                .filter(Apply.class::isInstance)
                .map(Apply.class::cast)
                .flatMap(apply -> lookup.lookup(apply.getLambda())
                        .map(Lambda.class::cast)
                        .filter(lambda -> isUncorrelatedScalar(lambda))
                        .flatMap(lambda -> lookup.lookup(apply.getInput())
                                .map(i -> process(lambda, i))));
    }

    private boolean isUncorrelatedScalar(Lambda lambda)
    {
        // TODO: need to resolve with lookup
        return lambda.getExpression() instanceof EnforceScalar && !containsReference(lambda.getExpression(), lambda.getVariable());
    }

    private boolean containsReference(Expression expression, String variable)
    {
        if (expression instanceof Reference) {
            return expression.getName().equals(variable);
        }

        return expression.getArguments().stream()
                .anyMatch(argument -> containsReference(argument, variable));
    }

    private Expression process(Lambda lambda, Expression input)
    {
        return new CrossJoin(input, lambda.getExpression());
    }
}
