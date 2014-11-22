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

import com.facebook.presto.sql.newplanner.optimizer.graph.Graph;
import com.facebook.presto.sql.newplanner.optimizer2.OptimizationResult;
import com.facebook.presto.sql.newplanner.optimizer2.OptimizedExpr;
import com.facebook.presto.sql.newplanner.optimizer2.Optimizer2;
import com.facebook.presto.sql.newplanner.optimizer2.OptimizerContext2;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.sql.newplanner.optimizer.RelExpr.Type.MERGE;
import static com.facebook.presto.sql.newplanner.optimizer.RelExpr.Type.REPARTITION;
import static com.facebook.presto.sql.newplanner.optimizer.RelExpr.Type.REPLICATE;

public class Main
{
    private static int nextNodeId = 0;

    public static void main(String[] args)
    {
        RelExpr expr =
                expression(RelExpr.Type.GROUPED_AGGREGATION, ImmutableList.of(2),
                        expression(RelExpr.Type.GROUPED_AGGREGATION, ImmutableList.of(1),
//                                expression(RelExpr.Type.LOCAL_GROUPED_AGGREGATION, ImmutableList.of(1),
                                        expression(RelExpr.Type.FILTER,
                                                expression(RelExpr.Type.PROJECT,
                                                        expression(RelExpr.Type.TABLE, ImmutableList.of()))))); //);

//        RelExpr expr =
//                expression(RelExpr.Type.HASH_JOIN, ImmutableList.of(1),
//                        ImmutableList.of(
//                                expression(RelExpr.Type.TABLE, ImmutableList.of(1)),
//                                expression(RelExpr.Type.TABLE, ImmutableList.of(1))));

//        RelExpr expr =
//                expression(RelExpr.Type.HASH_JOIN, ImmutableList.of(1),
//                        ImmutableList.of(
//                                expression(RelExpr.Type.HASH_JOIN, ImmutableList.of(1),
//                                        ImmutableList.of(
//                                                expression(RelExpr.Type.TABLE, ImmutableList.of(1)),
//                                                expression(RelExpr.Type.TABLE, ImmutableList.of(1)))),
//                                        expression(RelExpr.Type.TABLE, ImmutableList.of(1))));

        Graph<String, String, String, String> graph = new Graph<>();

        Optimizer2 optimizer = new Optimizer2();
        OptimizationResult optimized = optimizer.optimize(expr, new OptimizerContext2(nextNodeId));

        graph.addNode("root", "shape=point");
        String optimizedId = nodeId(optimized.getRequestedExpressionId(), optimized.getRequestedProperties());

        add(graph, optimized);

        graph.addEdge("root", optimizedId, "label=\"" + optimized.getBest().getProperties() + "\"");


        System.out.println(graph.toGraphviz(Functions.<String>identity(), Functions.<String>identity(), Functions.<String>identity()));
    }

    private static void add(Graph<String, String, String, String> graph, OptimizationResult parent)
    {
        String parentId = nodeId(parent.getRequestedExpressionId(), parent.getRequestedProperties());
        if (!graph.getNode(parentId).isPresent()) {
//            graph.addNode(parentId, "label=\"OPT(" + parent.getRequestedExpressionId() + ", " + parent.getRequestedProperties() + ")\"");
            graph.addNode(parentId, "shape=point");

            for (OptimizedExpr alternative : parent.getAlternatives()) {
                String alternativeId = nodeId(alternative);

                if (alternative == parent.getBest()) {
                    graph.addNode(alternativeId, "label=\"" + alternative.getType() + " (" + alternative.getId() + ")\",style=filled,fillcolor=salmon");
                }
                else {
                    graph.addNode(alternativeId, "label=\"" + alternative.getType() + " (" + alternative.getId() + ")\"");
                }

                graph.addEdge(parentId, alternativeId, "label=\"" + alternative.getProperties() + "\",style=dotted,arrowhead=none");

                for (OptimizationResult input : alternative.getInputs()) {
                    add(graph, input);

                    graph.addEdge(alternativeId, nodeId(input.getRequestedExpressionId(), input.getRequestedProperties()), "label=\"OPT(" + input.getRequestedExpressionId() + ", " + input.getRequestedProperties() + ")\"");
                }
            }
        }
    }

    private static String nodeId(int id, PhysicalConstraints requestedProperties)
    {
        return "\"" + id + ":" + requestedProperties + "\"";
    }

    private static void add(Graph<String, String, String, String> graph, OptimizedExpr expression)
    {
        String parentNodeId = nodeId(expression);
        if (!graph.getNode(parentNodeId).isPresent()) {
            List<String> attributes = new ArrayList<>();
            attributes.add("label=\"" + nodeLabel(expression) + "\"");

            graph.addNode(parentNodeId, Joiner.on(",").join(attributes));
        }

        List<OptimizationResult> inputs = expression.getInputs();
        for (int i = 0; i < inputs.size(); i++) {
            OptimizationResult child = inputs.get(i);

            for (OptimizedExpr alternative : child.getAlternatives()) {
                add(graph, alternative);

                String childNodeId = nodeId(alternative);
                List<String> attributes = new ArrayList<>();

                if (expression.getType() == MERGE || alternative.getType() == REPARTITION || alternative.getType() == REPLICATE) {
                    attributes.add("style=dashed");
                }

                if (expression.getProperties().isPartitioned() && !alternative.getProperties().isPartitioned()) {
                    attributes.add("arrowhead=none");
                    attributes.add("arrowtail=crow");
                }
                else if (!expression.getProperties().isPartitioned() && alternative.getProperties().isPartitioned()) {
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
    }

    private static String nodeLabel(OptimizedExpr expression)
    {
        return expression.getType() + " (" + expression.getId() + ")\\n" + expression.getProperties();
    }

    private static String nodeId(OptimizedExpr expression)
    {
//        long hash = Math.abs(XxHash64.hash(Slices.utf8Slice(expression.getProperties().toString())));
        return "\"" + expression.hashCode() + "\"";
    }

    private static RelExpr expression(RelExpr.Type type, Object payload, RelExpr input)
    {
        return new RelExpr(nextNodeId++, type, payload, ImmutableList.of(input));
    }

    private static RelExpr expression(RelExpr.Type type, Object payload, List<RelExpr> inputs)
    {
        return new RelExpr(nextNodeId++, type, payload, inputs);
    }

    private static RelExpr expression(RelExpr.Type type, RelExpr input)
    {
        return new RelExpr(nextNodeId++, type, null, ImmutableList.of(input));
    }

    private static RelExpr expression(RelExpr.Type type, Object payload)
    {
        return new RelExpr(nextNodeId++, type, payload, ImmutableList.<RelExpr>of());
    }

    private static RelExpr expression(RelExpr.Type type)
    {
        return new RelExpr(nextNodeId++, type);
    }

    private static void dump(OptimizedExpr expression, int indent)
    {
//        System.out.println(Utils.indent(indent) + expression.getId() + ":" + expression.getType() + " => " + expression.getProperties());
//        for (OptimizedExpr input : expression.getInputs()) {
//            dump(input, indent + 1);
//        }
    }
}
