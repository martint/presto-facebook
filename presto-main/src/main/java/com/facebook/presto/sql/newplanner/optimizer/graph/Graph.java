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
package com.facebook.presto.sql.newplanner.optimizer.graph;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Graph<V, E>
{
    private final Map<Integer, V> nodes = new HashMap<>();
    private final Map<Edge, E> edges = new HashMap<>();
    private final Map<Integer, Integer> clusters = new HashMap<>();

    public void addNode(int id, int cluster, V value)
    {
        if (nodes.containsKey(id)) {
            return;
        }

        nodes.put(id, value);
        clusters.put(id, cluster);
    }

    public void addNode(int id, V value)
    {
        if (nodes.containsKey(id)) {
            return;
        }

        nodes.put(id, value);
    }

    public void addEdge(int from, int to, E type)
    {
        Edge edge = new Edge(from, to, false);
        edges.put(edge, type);
    }

    public void addEdge(int from, int to, E type, boolean toCluster)
    {
        Edge edge = new Edge(from, to, toCluster);
        edges.put(edge, type);
    }

    public String toGraphviz(Function<V, String> nodeFormatter, Function<E, String> edgeFormatter)
    {
        StringBuilder builder = new StringBuilder("digraph G {\n");
        builder.append("\tcompound=true;");
        builder.append("\tranksep=1.5;");
        builder.append("\tnode [shape=rectangle];");

        Multimap<Integer, Integer> membership = HashMultimap.create();
        for (Map.Entry<Integer, Integer> entry : clusters.entrySet()) {
            membership.put(entry.getValue(), entry.getKey());
        }

        for (Map.Entry<Integer, Collection<Integer>> cluster : membership.asMap().entrySet()) {
            builder.append("\tsubgraph cluster_" + cluster.getKey() + "{\n");
            builder.append(String.format("\t\tlabel=\"%s\";\n", cluster.getKey()));
            builder.append(String.format("\t\t{rank=same; %s}\n", Joiner.on(" ").join(cluster.getValue())));

            for (int nodeId : cluster.getValue()) {
                V node = nodes.get(nodeId);
                builder.append("\t\t" + nodeId + " [" + nodeFormatter.apply(node) + "];\n");
            }

            builder.append("\t}\n");
        }

        for (Map.Entry<Edge, E> entry : edges.entrySet()) {
            int from = entry.getKey().from;
            int to = entry.getKey().to;
            if (entry.getKey().toCluster) {
                builder.append(String.format("\t%s -> %s [%s, lhead=cluster_%s];\n", from, to, edgeFormatter.apply(entry.getValue()), clusters.get(to)));
            }
            else {
                builder.append(String.format("\t%s -> %s [%s];\n", from, to, edgeFormatter.apply(entry.getValue())));
            }
        }

        builder.append("}");
        return builder.toString();
    }

    public int getCluster(int nodeId)
    {
        return clusters.get(nodeId);
    }

    private static final class Edge
    {
        private final int from;
        private final int to;
        private final boolean toCluster;

        public Edge(int from, int to, boolean toCluster)
        {
            this.from = from;
            this.to = to;
            this.toCluster = toCluster;
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

            Edge edge = (Edge) o;

            if (from != edge.from) {
                return false;
            }
            if (to != edge.to) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = from;
            result = 31 * result + to;
            return result;
        }
    }
}
