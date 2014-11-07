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

import com.facebook.presto.sql.newplanner.expression.OptimizationRequestExpression;
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

    private final Map<ExpressionWithRequirements, RelExpr> memoized = new HashMap<>();
    private final Map<GroupWithProperties, Integer> implementationClusters = new HashMap<>();

    public OptimizerContext(RelExpr seed)
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

    public Optional<RelExpr> getOptimized(RelExpr expression, PhysicalConstraints requirements)
    {
        RelExpr result = memoized.get(new ExpressionWithRequirements(expression, requirements));
        return Optional.fromNullable(result);
    }

    public void recordOptimization(RelExpr expression, PhysicalConstraints requirements, RelExpr optimized)
    {
        RelExpr previous = memoized.put(new ExpressionWithRequirements(expression, requirements), optimized);

        checkArgument(previous == null, "Optimization request already recorded");
    }

    public void recordExpression(RelExpr expression)
    {
        Queue<RelExpr> queue = new ArrayQueue<>();
        queue.add(expression);

        if (!graph.getNode(expression.getId()).isPresent()) {
            int cluster = nextClusterId();
            graph.addCluster(cluster, new ClusterInfo(cluster, null));
            graph.addNode(expression.getId(), cluster, new NodeInfo(expression, NodeInfo.Type.LOGICAL));
        }

        while (!queue.isEmpty()) {
            RelExpr current = queue.poll();

            for (RelExpr child : current.getInputs()) {
                if (child.getType() == RelExpr.Type.OPTIMIZE) {
                    int childCluster = implementationClusters.get(new GroupWithProperties(request.getGroup(), request.getRequirements()));
                    int nodeId = nextExpressionId();
                    graph.addNode(nodeId, childCluster, new NodeInfo(request, NodeInfo.Type.DUMMY));
                    graph.addEdge(current.getId(), nodeId, EdgeInfo.child(), true);
                }
                else {
                    if (!graph.getNode(child.getId()).isPresent()) {
                        int childCluster = nextClusterId();
                        graph.addCluster(childCluster, new ClusterInfo(childCluster, null));
                        graph.addNode(child.getId(), childCluster, new NodeInfo(child, NodeInfo.Type.LOGICAL));
                    }

                    graph.addEdge(current.getId(), child.getId(), EdgeInfo.child());
                    queue.add(child);
                }
            }
        }
    }

    public void recordLogicalTransform(RelExpr from, RelExpr to, ExplorationRule rule)
    {
        int cluster = graph.getCluster(from.getId());

        graph.addNode(to.getId(), cluster, new NodeInfo(to, NodeInfo.Type.LOGICAL));
        graph.addEdge(from.getId(), to.getId(), EdgeInfo.exploration(rule));

        recordExpression(to);
    }

    public void recordImplementation(RelExpr from, RelExpr to, PhysicalConstraints requirements, ImplementationRule rule)
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
                if (input.type == NodeInfo.Type.DUMMY) {
                    return "label=\"\",margin=0,width=0,height=0,style=invisible";
                }
                RelExpr expression = input.expression;

                String color = "black";
                switch (input.type) {
                    case LOGICAL:
                        color = "lightblue";
                        break;
                    case IMPLEMENTATION:
                        color = "salmon";
                        break;
                }
                String name = expression.getClass().getSimpleName().replace("Expression", "");
                return String.format("label=\"%s (%s)\",fillcolor=%s,style=filled", name, expression.getId(), color);
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
                        label = input.implementationRule.get().getClass().getSimpleName().replace("Rule", "");
                        break;
                    case EXPLORATION:
                        color = "blue";
                        label = input.explorationRule.get().getClass().getSimpleName().replace("Rule", "");
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

    private static int findMaxId(RelExpr seed)
    {
        int max = seed.getId();

        for (RelExpr child : seed.getInputs()) {
            max = Math.max(max, findMaxId(child));
        }

        return max;
    }

    public int getGroup(RelExpr expression)
    {
        return graph.getCluster(expression.getId());
    }

    private static class NodeInfo
    {
        private final RelExpr expression;
        private final Type type;

        public enum Type
        {
            LOGICAL,
            IMPLEMENTATION,
            DUMMY
        }

        public NodeInfo(RelExpr expression, Type type)
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
        private final PhysicalConstraints properties;
        private final int id;

        public ClusterInfo(int id, PhysicalConstraints properties)
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
        private final RelExpr expression;
        private final PhysicalConstraints requirements;

        public ExpressionWithRequirements(RelExpr expression, PhysicalConstraints requirements)
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

            if (expression.getId() != that.expression.getId()) {
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
            int result = expression.getId();
            result = 31 * result + requirements.hashCode();
            return result;
        }
    }

    private static final class GroupWithProperties
    {
        private final int cluster;
        private final PhysicalConstraints requirements;

        public GroupWithProperties(int cluster, PhysicalConstraints requirements)
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
