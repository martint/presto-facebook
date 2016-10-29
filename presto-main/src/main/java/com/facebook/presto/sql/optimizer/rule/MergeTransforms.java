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
import com.facebook.presto.sql.optimizer.tree.Expressions;

import java.util.stream.Stream;

import static com.facebook.presto.sql.optimizer.engine.Patterns.isCall;
import static com.facebook.presto.sql.optimizer.tree.Expressions.call;
import static com.facebook.presto.sql.optimizer.tree.Expressions.lambda;
import static com.facebook.presto.sql.optimizer.tree.Expressions.localReference;

public class MergeTransforms
        implements Rule
{
    @Override
    public Stream<Expression> transform(Expression expression, Lookup lookup)
    {
        if (!isCall(expression, "transform", lookup)) {
            return Stream.empty();
        }

        Apply parent = (Apply) expression;

        if (!isCall(parent.getArguments().get(0), "transform", lookup)) {
            return Stream.empty();
        }

        Apply child = (Apply) lookup.resolve(parent.getArguments().get(0));

        /*
            (map (map e g) f)

            =>

            (map e (lambda (x) (f (g x))))

            =>

            (map e (lambda (x) (let t (g x)) (f t)))   -- need a temporary "t" because of potential impure terms in f and g
         */

//        return new Transform(source,
//                lambda(let(
//                        // TODO: pick unique name
//                        list(new Assignment("t", Expressions.apply(childLambda, localReference()))),
//                        Expressions.apply(parentLambda, variable("t")))));

        return Stream.of(call(
                null,
                "transform",
                child.getArguments().get(0),
                lambda(
                        Expressions.apply(null, parent.getArguments().get(1),
                                Expressions.apply(null, child.getArguments().get(1), localReference(null))))));
    }
}
