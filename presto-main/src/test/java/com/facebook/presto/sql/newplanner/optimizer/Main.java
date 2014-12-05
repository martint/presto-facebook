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
import com.facebook.presto.sql.newplanner.optimizer2.Optimizer2;
import com.facebook.presto.sql.newplanner.optimizer2.OptimizerContext2;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.sql.newplanner.optimizer.RelExpr.Type.MERGE;
import static com.facebook.presto.sql.newplanner.optimizer.RelExpr.Type.OPTIMIZE;
import static com.facebook.presto.sql.newplanner.optimizer.RelExpr.Type.REPARTITION;
import static com.facebook.presto.sql.newplanner.optimizer.RelExpr.Type.REPLICATE;

public class Main
{
    private static int nextNodeId = 0;

    public static void main(String[] args)
    {
//        RelExpr expr =
//                expression(RelExpr.Type.GROUPED_AGGREGATION, ImmutableList.of(2),
//                        expression(RelExpr.Type.GROUPED_AGGREGATION, ImmutableList.of(1),
//                                expression(RelExpr.Type.LOCAL_GROUPED_AGGREGATION, ImmutableList.of(1),
//                                        expression(RelExpr.Type.FILTER,
//                                                expression(RelExpr.Type.PROJECT,
//                                                        expression(RelExpr.Type.TABLE, ImmutableList.of()))))));

//        RelExpr expr =
//                expression(RelExpr.Type.HASH_JOIN, ImmutableList.of(1),
//                        ImmutableList.of(
//                                expression(RelExpr.Type.TABLE, ImmutableList.of()),
//                                expression(RelExpr.Type.TABLE, ImmutableList.of())));

        RelExpr expr =
                expression(RelExpr.Type.GROUPED_AGGREGATION, ImmutableList.of(2),
                        expression(RelExpr.Type.HASH_JOIN, ImmutableList.of(1),
                                ImmutableList.of(
                                        expression(RelExpr.Type.HASH_JOIN, ImmutableList.of(1),
                                                ImmutableList.of(
                                                        expression(RelExpr.Type.TABLE, ImmutableList.of(1)),
                                                        expression(RelExpr.Type.TABLE, ImmutableList.of(2)))),
                                        expression(RelExpr.Type.TABLE, ImmutableList.of(1)))));

        Graph<String, String, String, String> graph = new Graph<>();

        Optimizer2 optimizer = new Optimizer2();
        RelExpr optimized = optimizer.optimize(expr, PhysicalConstraints.any(), new OptimizerContext2(nextNodeId));

//        graph.addNode("root", "shape=point");
//        String optimizedId = nodeId(optimized.getId(), PhysicalConstraints.any());

        add(graph, optimized, Optional.<RelExpr>empty(), true);
        add(graph, optimized, Optional.<RelExpr>empty(), false);

//        graph.addEdge("root", optimizedId, "label=\"OPT(" + expr.getId() + ", " + PhysicalConstraints.any() + ")\"");
//
        System.out.println(graph.toGraphviz(Functions.<String>identity(), Functions.<String>identity(), Functions.<String>identity()));
    }

    private static void add(Graph<String, String, String, String> graph, RelExpr node, Optional<RelExpr> parent, boolean onlyBest)
    {
        String nodeId = nodeId(node);

        boolean exists = graph.getNode(nodeId).isPresent();

        if (!exists) {
            List<String> nodeAttributes = new ArrayList<>();

            nodeAttributes.add("label=\"" + nodeLabel(node) + "\"");
            if (node.getType() == OPTIMIZE) {
                nodeAttributes.add("shape=point");
            }

            graph.addNode(nodeId, Joiner.on(',').join(nodeAttributes));
        }

        // recurse to add child nodes
        if (node.getType() == OPTIMIZE) {
            OptimizationResult optimization = (OptimizationResult) node.getPayload();
            for (RelExpr alternative : optimization.getAlternatives()) {
                if (alternative == optimization.getBest() && onlyBest || !onlyBest) {
                    add(graph, alternative, Optional.of(node), onlyBest);
                }
            }
        }
        else {
            for (RelExpr child : node.getInputs()) {
                add(graph, child, Optional.of(node), onlyBest);
            }
        }

        // add edges
        if (parent.isPresent()) {
            RelExpr parentNode = parent.get();

            if (node.getType() == OPTIMIZE) {
                OptimizationResult optimization = (OptimizationResult) node.getPayload();

                List<String> edgeAttributes = new ArrayList<>();
                edgeAttributes.add("label=\"OPT(" + optimization.getRequestedProperties() + ")\"");
                if (onlyBest) {
                    edgeAttributes.add("color=red");
                }

                if (graph.getEdge(nodeId(parentNode), nodeId) == null) {
                    graph.addEdge(nodeId(parentNode), nodeId, Joiner.on(",").join(edgeAttributes));
                }
            }
            else {
                List<String> edgeAttributes = new ArrayList<>();
                if (parentNode.getType() == MERGE || node.getType() == REPARTITION || node.getType() == REPLICATE) {
                    edgeAttributes.add("style=dashed");
                }

                if (parentNode.getType() == OPTIMIZE) {
                    OptimizationResult optimization = (OptimizationResult) parentNode.getPayload();

                    if (node == optimization.getBest() && onlyBest) {
                        edgeAttributes.add("color=red");
                    }
                }
                else if (onlyBest) {
                    edgeAttributes.add("color=red");
                }

                if (parentNode.getProperties().get().isPartitioned() && !node.getProperties().get().isPartitioned()) {
                    edgeAttributes.add("arrowhead=none");
                    edgeAttributes.add("arrowtail=crow");
                }
                else if (!parentNode.getProperties().get().isPartitioned() && node.getProperties().get().isPartitioned()) {
                    edgeAttributes.add("arrowhead=crow");
                    edgeAttributes.add("arrowtail=none");
                }
                else {
                    edgeAttributes.add("arrowhead=none");
                    edgeAttributes.add("arrowtail=none");
                }

                if (graph.getEdge(nodeId(parentNode), nodeId) == null) {
                    graph.addEdge(nodeId(parentNode), nodeId, Joiner.on(",").join(edgeAttributes));
                }
            }
        }
    }

    private static String nodeLabel(RelExpr expression)
    {
        return expression.getType() + " (" + expression.getId() + ")\\n" + expression.getProperties().get();
    }

    private static String nodeId(RelExpr expression)
    {
        return "" + Objects.hash(expression.getId(), expression.getType(), expression.getPayload(), expression.getProperties(), expression.getInputs());
    }

    private static RelExpr expression(RelExpr.Type type, Object payload, RelExpr input)
    {
        return new RelExpr(nextNodeId++, type, payload, ImmutableList.of(input), null);
    }

    private static RelExpr expression(RelExpr.Type type, Object payload, List<RelExpr> inputs)
    {
        return new RelExpr(nextNodeId++, type, payload, inputs, null);
    }

    private static RelExpr expression(RelExpr.Type type, RelExpr input)
    {
        return new RelExpr(nextNodeId++, type, null, ImmutableList.of(input), null);
    }

    private static RelExpr expression(RelExpr.Type type, Object payload)
    {
        return new RelExpr(nextNodeId++, type, payload, ImmutableList.<RelExpr>of(), null);
    }

    private static RelExpr expression(RelExpr.Type type)
    {
        return new RelExpr(nextNodeId++, type);
    }

//    private static void dump(OptimizedExpr expression, int indent)
//    {
////        System.out.println(Utils.indent(indent) + expression.getId() + ":" + expression.getType() + " => " + expression.getProperties());
////        for (OptimizedExpr input : expression.getInputs()) {
////            dump(input, indent + 1);
////        }
//    }
}
