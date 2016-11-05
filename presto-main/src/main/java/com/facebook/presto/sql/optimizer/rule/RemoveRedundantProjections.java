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
import com.facebook.presto.sql.optimizer.tree.Reference;
import com.facebook.presto.sql.optimizer.tree.Value;

import java.util.stream.Stream;

import static com.facebook.presto.sql.optimizer.engine.Patterns.isCall;

// collapses redundant row calls followed by field-deref
public class RemoveRedundantProjections
        implements Rule
{
    @Override
    public Stream<Expression> transform(Expression expression, Lookup lookup)
    {
        if (!isCall(expression, "field-deref", lookup)) {
            return Stream.empty();
        }

        Apply apply = (Apply) expression;
        if (!isCall(lookup.resolve(apply.getArguments().get(0)), "row", lookup)) {
            return Stream.empty();
        }

        Value field = (Value) lookup.resolve(apply.getArguments().get(1));
        if (!(field.getValue() instanceof Number)) {
            return Stream.empty();
        }

        Apply row = (Apply) lookup.resolve(apply.getArguments().get(0));
        return Stream.of(row.getArguments().get(((Number) field.getValue()).intValue()));
    }
}
