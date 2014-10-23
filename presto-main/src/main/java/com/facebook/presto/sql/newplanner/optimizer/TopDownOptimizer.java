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
package com.facebook.presto.sql.newplanner.optimizer;

import com.facebook.presto.sql.newplanner.expression.RelationalExpression;
import com.google.common.base.Optional;
import org.eclipse.jetty.util.ArrayQueue;

import java.util.List;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkNotNull;

public class TopDownOptimizer
{
    private final List<OptimizerRule> rules;

    public TopDownOptimizer(List<OptimizerRule> rules)
    {
        checkNotNull(rules, "rules is null");

        this.rules = rules;
    }

    // TODO: memoize
    public RelationalExpression optimize(RelationalExpression expression, ExpressionProperties requiredProperties)
    {
        Queue<RelationalExpression> equivalences = new ArrayQueue<>();
        equivalences.add(expression);

        while (!equivalences.isEmpty()) {
            RelationalExpression current = equivalences.poll();

            // first, compute all possible transforms for this expression and queue them up for optimization
            for (OptimizerRule rule : rules) {
                Optional<RelationalExpression> transformed = rule.apply(current);
                if (!transformed.isPresent()) {
                    continue;
                }

                equivalences.add(transformed.get());
            }

            // next, compute the require properties for the child given the semantics of the outer operation and the
            // expected properties
            ExpressionProperties requiredChildProperties = current.computeRequiredInputProperties(requiredProperties);
            // TODO: optimize children

        }
    }
}
