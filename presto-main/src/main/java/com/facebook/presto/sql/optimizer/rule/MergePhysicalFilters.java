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
        return lookup.resolve(expression)
                .filter(isCall("physical-filter"))
                .map(Apply.class::cast)
                .flatMap(parent ->
                        lookup.resolve(parent.getArguments().get(0))
                                .filter(isCall("physical-filter"))
                                .map(Apply.class::cast)
                                .flatMap(child ->
                                        lookup.resolve(parent.getArguments().get(1))
                                                .flatMap(parentLambda ->
                                                        lookup.resolve(child.getArguments().get(1))
                                                                .map(childLambda ->
                                                                        process(child.getArguments().get(0), (Lambda) parentLambda, (Lambda) childLambda)))));
    }

    private Expression process(Expression source, Lambda parentLambda, Lambda childLambda)
    {
        return call("physical-filter",
                source,
                lambda(call("and",
                        childLambda.getBody(),
                                parentLambda.getBody())));
    }
}
