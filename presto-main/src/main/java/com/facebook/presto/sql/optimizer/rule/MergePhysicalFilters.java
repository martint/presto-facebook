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
import static com.facebook.presto.sql.optimizer.tree.Expressions.call;
import static com.facebook.presto.sql.optimizer.tree.Expressions.lambda;

public class MergePhysicalFilters
        implements Rule
{
    @Override
    public Stream<Expression> apply(Expression expression, Lookup lookup)
    {
        if (!isCall(expression, "physical-filter", lookup)) {
            return Stream.empty();
        }

        Apply parent = (Apply) expression;

        if (!isCall(parent.getArguments().get(0), "physical-filter", lookup)) {
            return Stream.empty();
        }

        Apply child = (Apply) parent.getArguments().get(0);

        return Stream.of(
                call("physical-filter",
                        child.getArguments().get(0),
                        lambda(call("and",
                                ((Lambda) child.getArguments().get(1)).getBody(),
                                ((Lambda) parent.getArguments().get(1)).getBody()))));
    }
}
