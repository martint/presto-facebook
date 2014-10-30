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
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.eclipse.jetty.util.ArrayQueue;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.isNull;

public class Optimizer2
{
    private final List<ExplorationRule> rules;

    public Optimizer2(List<ExplorationRule> rules)
    {
        checkNotNull(rules, "rules is null");

        this.rules = rules;
    }

    public RelationalExpression optimize(RelationalExpression expression)
    {
        Blackboard blackboard = new Blackboard(expression);

        final ExpressionProperties requiredProperties = null; // TODO: what are the required properties for input expression?
        int group = blackboard.getRootGroup();

        // first, compute transitive closure of all possible transformations of the current group
        Queue<RelationalExpression> candidates = new ArrayQueue<>(blackboard.getExpressionsInGroup(group));
        while (!candidates.isEmpty()) {
            RelationalExpression candidate = candidates.poll();
            for (ExplorationRule rule : rules) {
                // match rule pattern against candidate expression and inputs
                // apply rule
                // incorporate transformed expression into blackboard
            }
        }

        // derive required properties for each operator in the current group
        for (RelationalExpression candidate : blackboard.getExpressionsInGroup(group)) {
            List<ExpressionProperties> requiredChildProperties = Lists.transform(expression.getInputs(), new Function<RelationalExpression, ExpressionProperties>() {
                @Nullable
                @Override
                public ExpressionProperties apply(RelationalExpression expression)
                {
                    return expression.computeRequiredInputProperties(requiredProperties);
                }
            });

            if (Iterables.any(requiredChildProperties, isNull())) {
                // only physical operators have requirements for their inputs TODO: maybe figure out a better way to determine this?
                continue;
            }

            // TODO: optimize each child
        }

        RelationalExpression result = null; // TODO: extract optimal expression from blackboard

        return result;
    }

}
