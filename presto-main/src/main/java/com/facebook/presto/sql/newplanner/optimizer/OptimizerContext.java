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

import com.facebook.presto.sql.newplanner.expression.EquivalenceGroupReferenceExpression;
import com.facebook.presto.sql.newplanner.expression.RelationalExpression;
import com.facebook.presto.sql.newplanner.optimizer.graph.Graph;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import org.eclipse.jetty.util.ArrayQueue;

import java.util.Queue;

public class OptimizerContext
{
    private int nextExpressionId;
    private final Graph<NodeInfo, EdgeInfo> graph = new Graph<>();

    public OptimizerContext(RelationalExpression seed)
    {
        nextExpressionId = findMaxId(seed) + 1;
    }

    public int nextExpressionId()
    {
        return nextExpressionId++;
    }

    public void recordExpression(RelationalExpression expression)
    {
        Queue<RelationalExpression> queue = new ArrayQueue<>();
        queue.add(expression);
        graph.addNode(expression.getId(), expression.getId(), new NodeInfo(expression, NodeInfo.Type.LOGICAL));

        while (!queue.isEmpty()) {
            RelationalExpression current = queue.poll();

            for (RelationalExpression child : current.getInputs()) {
                if (child instanceof EquivalenceGroupReferenceExpression) {
                    graph.addEdge(current.getId(), ((EquivalenceGroupReferenceExpression) child).getGroupId(), EdgeInfo.child(), true);
                }
                else {
                    graph.addNode(child.getId(), child.getId(), new NodeInfo(child, NodeInfo.Type.LOGICAL));
                    graph.addEdge(current.getId(), child.getId(), EdgeInfo.child());
                    queue.add(child);
                }
            }
        }
    }

    public void recordLogicalTransform(RelationalExpression from, RelationalExpression to, ExplorationRule rule)
    {
        int cluster = graph.getCluster(from.getId());

        graph.addNode(to.getId(), cluster, new NodeInfo(to, NodeInfo.Type.LOGICAL));
        graph.addEdge(from.getId(), to.getId(), EdgeInfo.exploration(rule));

        recordExpression(to);
    }

    public void recordImplementation(RelationalExpression from, RelationalExpression to, ExpressionProperties requirements, ImplementationRule rule)
    {
        int cluster = graph.getCluster(from.getId());

        graph.addNode(to.getId(), cluster, new NodeInfo(to, NodeInfo.Type.IMPLEMENTATION));
        graph.addEdge(from.getId(), to.getId(), EdgeInfo.implementation(rule));
    }

    public String expressionsToGraphviz()
    {
        return graph.toGraphviz(new Function<NodeInfo, String>()
        {
            @Override
            public String apply(NodeInfo input)
            {
                RelationalExpression expression = input.expression;

                String color="black";
                switch (input.type) {
                    case LOGICAL:
                        color = "lightblue";
                        break;
                    case IMPLEMENTATION:
                        color = "salmon";
                        break;
                }
                return String.format("label=\"%s (%s)\",fillcolor=%s,style=filled", expression.getClass().getSimpleName(), expression.getId(), color);
            }
        }, new Function<EdgeInfo, String>()
        {
            @Override
            public String apply(EdgeInfo input)
            {
                String color = "black";
                String label = "";
                switch (input.type) {
                    case IMPLEMENTATION:
                        color = "red";
                        label = input.implementationRule.get().getClass().getSimpleName();
                        break;
                    case EXPLORATION:
                        color = "blue";
                        label = input.explorationRule.get().getClass().getSimpleName();
                        break;
                }

                return String.format("color=%s, label=\"%s\"", color, label);
            }
        });
    }

    private static int findMaxId(RelationalExpression seed)
    {
        int max = seed.getId();

        for (RelationalExpression child : seed.getInputs()) {
            max = Math.max(max, findMaxId(child));
        }

        return max;
    }

    public int getGroup(RelationalExpression expression)
    {
        return graph.getCluster(expression.getId());
    }

    private static class NodeInfo
    {
        private final RelationalExpression expression;
        private final Type type;

        public enum Type {
            LOGICAL,
            IMPLEMENTATION
        }

        public NodeInfo(RelationalExpression expression, Type type)
        {
            this.expression = expression;
            this.type = type;
        }
    }

    private static class EdgeInfo
    {
        private final Type type;
        private final Optional<ImplementationRule> implementationRule;
        private final Optional<ExplorationRule> explorationRule;

        public enum Type {
            IMPLEMENTATION,
            EXPLORATION,
            CHILD,
        }

        private EdgeInfo(Type type, Optional<ImplementationRule> implementationRule, Optional<ExplorationRule> explorationRule)
        {
            this.type = type;
            this.implementationRule = implementationRule;
            this.explorationRule = explorationRule;
        }

        public static EdgeInfo implementation(ImplementationRule rule)
        {
            return new EdgeInfo(Type.IMPLEMENTATION, Optional.of(rule), Optional.<ExplorationRule>absent());
        }

        public static EdgeInfo exploration(ExplorationRule rule)
        {
            return new EdgeInfo(Type.EXPLORATION, Optional.<ImplementationRule>absent(), Optional.of(rule));
        }

        public static EdgeInfo child()
        {
            return new EdgeInfo(Type.CHILD, Optional.<ImplementationRule>absent(), Optional.<ExplorationRule>absent());
        }
    }
}
