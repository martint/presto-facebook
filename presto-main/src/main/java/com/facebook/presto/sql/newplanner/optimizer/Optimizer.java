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

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class Optimizer
{
    private final List<ImplementationRule> implementationRules = new ArrayList<>();
    private final List<ExplorationRule> explorationRules = new ArrayList<>();

    public RelationalExpression optimize(RelationalExpression expression)
    {
        return optimize(expression, ExpressionProperties.UNPARTITIONED);
    }

    private RelationalExpression optimize(RelationalExpression expression, ExpressionProperties requirements)
    {
        // apply exploration rules, queue up optimization calls
        Queue<RelationalExpression> explorationCandidates = new ArrayQueue<>();
        explorationCandidates.add(expression);

        Queue<RelationalExpression> implementationCandidates = new ArrayQueue<>();
        while (!explorationCandidates.isEmpty()) {
            RelationalExpression current = explorationCandidates.poll();
            implementationCandidates.add(current);

            for (ExplorationRule rule : explorationRules) {
                Optional<RelationalExpression> transformed = rule.apply(current);
                if (transformed.isPresent()) {
                    explorationCandidates.add(transformed.get());
                }
            }
        }

        List<RelationalExpression> candidatePlans = new ArrayList<>();
        while (!implementationCandidates.isEmpty()) {
            RelationalExpression current = implementationCandidates.poll();

            for (ImplementationRule rule : implementationRules) {
                Optional<RelationalExpression> implementation = rule.implement(current, requirements, this);
                if (implementation.isPresent()) {
                    candidatePlans.add(implementation.get());
                }
            }
        }

        System.out.println(candidatePlans);
        return null;
    }
}
