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
import com.facebook.presto.sql.optimizer.tree.CrossJoin;
import com.facebook.presto.sql.optimizer.tree.EnforceScalar;
import com.facebook.presto.sql.optimizer.tree.Expression;

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
                .filter(e -> e.getLambda().getVariable().equals("u") && e.getLambda().getExpression() instanceof EnforceScalar)
                .map(this::process);

    }

    private Expression process(Apply apply)
    {
        return new CrossJoin(
                apply.getArguments().get(0),
                apply.getLambda().getExpression());
    }
}
