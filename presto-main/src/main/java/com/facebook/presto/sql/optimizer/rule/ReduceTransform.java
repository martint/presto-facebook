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
import com.facebook.presto.sql.optimizer.tree.type.RelationTypeStamp;
import com.facebook.presto.sql.optimizer.tree.type.RowTypeStamp;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.facebook.presto.sql.optimizer.engine.Patterns.isCall;
import static com.facebook.presto.sql.optimizer.tree.Expressions.call;

/**
 * Transforms:
 * <p>
 * (transform (array e1 ... en) lambda)
 * <p>
 * =>
 * <p>
 * (array ((lambda) e1) ... ((lambda) en)
 */
public class ReduceTransform
        implements Rule
{
    @Override
    public Stream<Expression> transform(Expression expression, Lookup lookup)
    {
        if (!isCall(expression, "transform", lookup)) {
            return Stream.empty();
        }

        Apply transform = (Apply) expression;
        Lambda lambda = (Lambda) lookup.resolve(transform.getArguments().get(1));

        Expression child = lookup.resolve(transform.getArguments().get(0));
        if (!isCall(child, "array", lookup)) {
            return Stream.empty();
        }

        Apply array = (Apply) child;

        List<Expression> newItems = new ArrayList<>();
        for (Expression item : array.getArguments()) {
            newItems.add(new Apply(
                    lambda.getBody().type(),
                    lambda,
                    ImmutableList.of(item)));
        }

        return Stream.of(call(
                new RelationTypeStamp((RowTypeStamp) lambda.getBody().type()),
                "array",
                newItems));
    }
}
