package com.facebook.presto.graph;

import com.google.common.base.Function;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GraphvizPrinter
{
    public static <N, E> String toGraphviz(Graph<N, E> graph, Function<N, Map<String, String>> nodeFormatter, Function<Edge<N, E>, Map<String, String>> edgeFormatter)
    {
        Map<N, Integer> ids = new HashMap<>();

        StringBuilder builder = new StringBuilder();

        builder.append("digraph G {\n");
        for (N node : graph.getNodes()) {
            Map<String, String> attributes = nodeFormatter.apply(node);

            Integer id = ids.get(node);
            if (id == null) {
                id = ids.size();
                ids.put(node, id);
            }

            builder.append(String.format("\t%s [%s];\n", id, Joiner.on(',').withKeyValueSeparator("=").join(attributes)));
        }

        for (N node : graph.getNodes()) {
            Set<Edge<N,E>> edges = graph.getOutgoingEdges(node);

            int fromId = ids.get(node);
            for (Edge<N, E> edge : edges) {
                Map<String, String> attributes = edgeFormatter.apply(edge);

                int toId = ids.get(edge.getTo());

                builder.append(String.format("\t%s -> %s [%s];\n", fromId, toId, Joiner.on(',').withKeyValueSeparator("=").join(attributes)));
            }
        }

        builder.append("}\n");

        return builder.toString();
    }
}
