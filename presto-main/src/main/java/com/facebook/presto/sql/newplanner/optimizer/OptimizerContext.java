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
import org.eclipse.jetty.util.ArrayQueue;

import java.util.List;
import java.util.Queue;

public class OptimizerContext
{
    private int nextExpressionId;
    private final Graph<NodeInfo, String> graph = new Graph<>();

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
        graph.addNode(expression.getId(), expression.getId(), new NodeInfo(expression, "logical"));

        while (!queue.isEmpty()) {
            RelationalExpression current = queue.poll();

            for (RelationalExpression child : current.getInputs()) {
                if (child instanceof EquivalenceGroupReferenceExpression) {
                    graph.addEdge(current.getId(), ((EquivalenceGroupReferenceExpression) child).getGroupId(), "color=black", true);
                }
                else {
                    graph.addNode(child.getId(), child.getId(), new NodeInfo(child, "logical"));
                    graph.addEdge(current.getId(), child.getId(), "color=black");
                    queue.add(child);
                }
            }
        }
    }

    public void recordExpressions(List<RelationalExpression> expressions)
    {
        for (RelationalExpression expression : expressions) {
            recordExpression(expression);
        }
    }

    public void recordLogicalTransform(RelationalExpression from, RelationalExpression to)
    {
        int cluster = graph.getCluster(from.getId());

        graph.addNode(to.getId(), cluster, new NodeInfo(to, "logical"));
        graph.addEdge(from.getId(), to.getId(), "color=blue");

        recordExpression(to);
    }

    public void recordImplementation(RelationalExpression from, RelationalExpression to)
    {
        int cluster = graph.getCluster(from.getId());

        graph.addNode(to.getId(), cluster, new NodeInfo(to, "implementation"));
        graph.addEdge(from.getId(), to.getId(), "color=red");
    }

    public String expressionsToGraphviz()
    {
        return graph.toGraphviz(new Function<NodeInfo, String>()
        {
            @Override
            public String apply(NodeInfo input)
            {
                RelationalExpression expression = input.expression;

                String color = input.type.equals("logical") ? "lightblue" : "salmon";
                return String.format("label=\"%s (%s)\",fillcolor=%s,style=filled", expression.getClass().getSimpleName(), expression.getId(), color);
            }
        }, new Function<String, String>()
        {
            @Override
            public String apply(String input)
            {
                return input;
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

    private class NodeInfo
    {
        private final RelationalExpression expression;
        private final String type;

        public NodeInfo(RelationalExpression expression, String type)
        {
            this.expression = expression;
            this.type = type;
        }
    }
}
