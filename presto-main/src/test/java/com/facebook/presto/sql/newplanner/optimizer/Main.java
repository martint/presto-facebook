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
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;

import java.util.ArrayList;
import java.util.List;

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
        graph.addNode("root", "shape=point");
        for (OptimizationResult result : optimizer.optimize(expr)) {
            add(graph, result);
            graph.addEdge("root", nodeId(result), "style=dotted,arrowhead=none");
//            dump(result, 0);
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
            List<String> attributes = new ArrayList<>();

//            attributes.add("arrowtail=none");
//            attributes.add("arrowhead=none");
//            attributes.add("penwidth=10");

            if (expression.getType() == RelExpr.Type.MERGE || child.getType() == RelExpr.Type.PARTITION) {
                attributes.add("style=dashed");
            }

//            if (expression.getProperties().isPartitioned() && child.getProperties().isPartitioned()) {
//                attributes.add("penwidth=7");
//            }
            if (expression.getProperties().isPartitioned() && !child.getProperties().isPartitioned()) {
                attributes.add("arrowhead=none");
                attributes.add("arrowtail=crow");
            }
            else if (!expression.getProperties().isPartitioned() && child.getProperties().isPartitioned()) {
                attributes.add("arrowhead=crow");
                attributes.add("arrowtail=none");
//                attributes.add("dir=back");
            }
            else {
                attributes.add("arrowhead=none");
                attributes.add("arrowtail=none");
            }

            graph.addEdge(parentNodeId, childNodeId, Joiner.on(",").join(attributes));
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
