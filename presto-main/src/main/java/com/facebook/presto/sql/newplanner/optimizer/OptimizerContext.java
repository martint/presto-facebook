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

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkArgument;

public class OptimizerContext
{
    private int nextExpressionId;
    private int nextClusterId;
    private final Graph<NodeInfo, EdgeInfo, ClusterInfo> graph = new Graph<>();

    private final Map<ExpressionWithRequirements, RelationalExpression> memoized = new HashMap<>();
    private final Map<GroupWithProperties, Integer> implementationClusters = new HashMap<>();

    public OptimizerContext(RelationalExpression seed)
    {
        nextExpressionId = findMaxId(seed) + 1;
    }

    public int nextExpressionId()
    {
        return nextExpressionId++;
    }

    public int nextClusterId()
    {
        return nextClusterId++;
    }

    public Optional<RelationalExpression> getOptimized(RelationalExpression expression, ExpressionProperties requirements)
    {
        RelationalExpression result = memoized.get(new ExpressionWithRequirements(expression, requirements));
        return Optional.fromNullable(result);
    }

    public void recordOptimization(RelationalExpression expression, ExpressionProperties requirements, RelationalExpression optimized)
    {
        RelationalExpression previous = memoized.put(new ExpressionWithRequirements(expression, requirements), optimized);

        checkArgument(previous == null, "Optimization request already recorded");
    }

    public int recordExpression(RelationalExpression expression)
    {
        Queue<RelationalExpression> queue = new ArrayQueue<>();
        queue.add(expression);

        int cluster = nextClusterId();
        graph.addCluster(cluster, new ClusterInfo(cluster, null));
        graph.addNode(expression.getId(), cluster, new NodeInfo(expression, NodeInfo.Type.LOGICAL));

        while (!queue.isEmpty()) {
            RelationalExpression current = queue.poll();

            for (RelationalExpression child : current.getInputs()) {
//                if (child instanceof EquivalenceGroupReferenceExpression) {
//                    graph.addEdge(current.getId(), ((EquivalenceGroupReferenceExpression) child).getGroupId(), EdgeInfo.child(), true);
//                }
//                else {
                    int childCluster = nextClusterId();
                    graph.addCluster(childCluster, new ClusterInfo(childCluster, null));

                    graph.addNode(child.getId(), childCluster, new NodeInfo(child, NodeInfo.Type.LOGICAL));
                    graph.addEdge(current.getId(), child.getId(), EdgeInfo.child());
                    queue.add(child);
//                }
            }
        }

        return cluster;
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
        int logicalGroup = graph.getCluster(from.getId());

        Integer implementationGroup = implementationClusters.get(new GroupWithProperties(logicalGroup, requirements));
        if (implementationGroup == null) {
            implementationGroup = nextClusterId();
            implementationClusters.put(new GroupWithProperties(logicalGroup, requirements), implementationGroup);
            graph.addCluster(implementationGroup, new ClusterInfo(implementationGroup, requirements));
        }

        graph.addNode(to.getId(), implementationGroup, new NodeInfo(to, NodeInfo.Type.IMPLEMENTATION));
        graph.addEdge(from.getId(), to.getId(), EdgeInfo.implementation(rule));

        recordExpression(to);
    }

    public String expressionsToGraphviz()
    {
        return graph.toGraphviz(new Function<NodeInfo, String>()
        {
            @Override
            public String apply(NodeInfo input)
            {
                RelationalExpression expression = input.expression;

                String color = "black";
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
        }, new Function<ClusterInfo, String>()
        {
            @Override
            public String apply(ClusterInfo input)
            {
                if (input.properties != null) {
                    return String.format("label=\"%s (%s)\"", input.properties, input.getId());
                }

                return String.format("label=\"(%s)\"", input.getId());
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

        public enum Type
        {
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

        public enum Type
        {
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

    private static class ClusterInfo
    {
        private final ExpressionProperties properties;
        private final int id;

        public ClusterInfo(int id, ExpressionProperties properties)
        {
            this.id = id;
            this.properties = properties;
        }

        public int getId()
        {
            return id;
        }
    }

    private static final class ExpressionWithRequirements
    {
        private final RelationalExpression expression;
        private final ExpressionProperties requirements;

        public ExpressionWithRequirements(RelationalExpression expression, ExpressionProperties requirements)
        {
            // TODO: cache expression hashcode
            this.expression = expression;
            this.requirements = requirements;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ExpressionWithRequirements that = (ExpressionWithRequirements) o;

            if (!expression.equals(that.expression)) {
                return false;
            }
            if (!requirements.equals(that.requirements)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = expression.hashCode();
            result = 31 * result + requirements.hashCode();
            return result;
        }
    }

    private static final class GroupWithProperties
    {
        private final int cluster;
        private final ExpressionProperties requirements;

        public GroupWithProperties(int cluster, ExpressionProperties requirements)
        {
            this.requirements = requirements;
            this.cluster = cluster;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            GroupWithProperties that = (GroupWithProperties) o;

            if (cluster != that.cluster) {
                return false;
            }
            if (!requirements.equals(that.requirements)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = cluster;
            result = 31 * result + requirements.hashCode();
            return result;
        }
    }
}
