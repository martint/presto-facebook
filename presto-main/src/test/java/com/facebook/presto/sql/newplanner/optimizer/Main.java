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

import com.facebook.presto.sql.newplanner.expression.Utils;
import com.facebook.presto.sql.newplanner.optimizer.graph.Graph;
import com.facebook.presto.sql.newplanner.optimizer2.OptimizationResult;
import com.facebook.presto.sql.newplanner.optimizer2.Optimizer2;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;

public class Main
{
    private static int nextNodeId = 0;

    public static void main(String[] args)
    {
        RelExpr expr =
                expression(RelExpr.Type.GROUPED_AGGREGATION, ImmutableList.of(2),
                        expression(RelExpr.Type.GROUPED_AGGREGATION, ImmutableList.of(1),
                                expression(RelExpr.Type.LOCAL_GROUPED_AGGREGATION, ImmutableList.of(1),
                                        expression(RelExpr.Type.FILTER,
                                                expression(RelExpr.Type.PROJECT,
                                                        expression(RelExpr.Type.TABLE))))));

        Graph<String, String, String, String> graph = new Graph<>();

        Optimizer2 optimizer = new Optimizer2();
        graph.addNode("root", "");
        for (OptimizationResult result : optimizer.optimize(expr)) {
            add(graph, result);
            graph.addEdge("root", nodeId(result), "");
            dump(result, 0);
        }

        System.out.println(graph.toGraphviz(Functions.<String>identity(), Functions.<String>identity(), Functions.<String>identity()));
    }

    private static void add(Graph<String, String, String, String> graph, OptimizationResult expression)
    {
        String parentNodeId = nodeId(expression);
        if (!graph.getNode(parentNodeId).isPresent()) {
            graph.addNode(parentNodeId, "label=\"" + nodeLabel(expression) + "\"");
        }

        for (OptimizationResult child : expression.getInputs()) {
            add(graph, child);
            String childNodeId = nodeId(child);
            graph.addEdge(parentNodeId, childNodeId, "");
        }
    }

    private static String nodeLabel(OptimizationResult expression)
    {
        return expression.getType() + " (" + expression.getId() + ")\\n" + expression.getProperties();
    }

    private static String nodeId(OptimizationResult expression)
    {
//        long hash = Math.abs(XxHash64.hash(Slices.utf8Slice(expression.getProperties().toString())));
        return "\"" + expression.hashCode() + "\"";
    }

    private static RelExpr expression(RelExpr.Type type, Object payload, RelExpr input)
    {
        return new RelExpr(nextNodeId++, type, payload, ImmutableList.of(input));
    }

    private static RelExpr expression(RelExpr.Type type, RelExpr input)
    {
        return new RelExpr(nextNodeId++, type, null, ImmutableList.of(input));
    }

    private static RelExpr expression(RelExpr.Type type)
    {
        return new RelExpr(nextNodeId++, type);
    }

    private static void dump(OptimizationResult expression, int indent)
    {
        System.out.println(Utils.indent(indent) + expression.getId() + ":" + expression.getType() + " => " + expression.getProperties());
        for (OptimizationResult input : expression.getInputs()) {
            dump(input, indent + 1);
        }
    }
}
