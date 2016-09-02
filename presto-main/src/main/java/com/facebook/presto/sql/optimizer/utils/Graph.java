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
package com.facebook.presto.sql.optimizer.utils;

import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;

public class Graph<VID, CID, V, E, C>
{
    private final Map<VID, V> nodes = new HashMap<>();
    private final Map<Edge<VID>, E> edges = new HashMap<>();
    private final Map<CID, C> clusters = new HashMap<>();
    private final Map<VID, CID> membership = new HashMap<>();
    private final Map<CID, Set<VID>> nodesByCluster = new HashMap<>();

    public int getNodeCount()
    {
        return nodes.size();
    }

    public void addNode(VID node, CID cluster, V value)
    {
        checkArgument(clusters.containsKey(cluster), "cluster does not exist: %s", cluster);
        checkArgument(!nodes.containsKey(node), "node already exists: %s", node);

        nodes.put(node, value);
        membership.put(node, cluster);
        nodesByCluster.get(cluster).add(node);
    }

    public void addNode(VID id, V value)
    {
        checkArgument(!nodes.containsKey(id), "node already exists: %s", id);
        nodes.put(id, value);
    }

    public void addNodeToCluster(VID node, CID cluster)
    {
        checkArgument(clusters.containsKey(cluster), "cluster does not exist: %s", cluster);
        checkArgument(nodes.containsKey(node), "node does not exist: %s", node);

        membership.put(node, cluster);
        nodesByCluster.get(cluster).add(node);
    }

    public void addEdge(VID from, VID to, E type)
    {
        addEdge(from, to, type, false);
    }

    public void addEdge(VID from, VID to, E type, boolean toCluster)
    {
        checkArgument(nodes.containsKey(from), "node does not exist: %s", from);
        checkArgument(nodes.containsKey(to), "node does not exist: %s", to);

        Edge<VID> edge = new Edge<>(from, to, toCluster);
        edges.put(edge, type);
    }

    public void addCluster(CID cluster, C value)
    {
        checkArgument(!clusters.containsKey(cluster), "cluster already exists: %s", cluster);
        clusters.put(cluster, value);
        nodesByCluster.put(cluster, new HashSet<>());
    }

    @FunctionalInterface
    public interface EdgeAttributeFunction<VID, E>
    {
        Map<String, String> apply(VID from, VID to, E edge);
    }

    public String toGraphviz(
            Supplier<Map<String, String>> graphAttributes,
            BiFunction<VID, V, Map<String, String>> nodeAttributes,
            EdgeAttributeFunction<VID, E> edgeAttributes,
            BiFunction<CID, C, List<String>> clusterFormatter)
    {
        StringBuilder builder = new StringBuilder("digraph G {\n");
        builder.append("\tcompound=true;\n");

        for (Map.Entry<String, String> entry : graphAttributes.get().entrySet()) {
            builder.append(String.format("\t%s=\"%s\";\n", entry.getKey(), entry.getValue()));
        }

        Set<VID> nodesWithoutCluster = new HashSet<>();
        Multimap<CID, VID> nodesByCluster = HashMultimap.create();
        for (VID nodeId : nodes.keySet()) {
            CID cluster = this.membership.get(nodeId);
            if (cluster != null) {
                nodesByCluster.put(cluster, nodeId);
            }
            else {
                nodesWithoutCluster.add(nodeId);
            }
        }

        for (Map.Entry<CID, Collection<VID>> entry : nodesByCluster.asMap().entrySet()) {
            builder.append("\tsubgraph cluster_" + entry.getKey() + "{\n");

            for (String item : clusterFormatter.apply(entry.getKey(), clusters.get(entry.getKey()))) {
                builder.append("\t\t" + item + ";\n");
            }

            for (VID nodeId : entry.getValue()) {
                V node = nodes.get(nodeId);
                builder.append("\t\t" + nodeId + " [" + format(nodeAttributes.apply(nodeId, node)) + "];\n");
            }

            builder.append("\t}\n");
        }

        for (VID nodeId : nodesWithoutCluster) {
            V node = nodes.get(nodeId);
            builder.append("\t\t" + nodeId + " [" + format(nodeAttributes.apply(nodeId, node)) + "];\n");
        }

        for (Map.Entry<Edge<VID>, E> entry : edges.entrySet()) {
            VID from = entry.getKey().from;
            VID to = entry.getKey().to;

            Map<String, String> attributes = new HashMap<>(edgeAttributes.apply(from, to, entry.getValue()));
            if (entry.getKey().toCluster) {
                attributes.put("lhead", "cluster_" + this.membership.get(to));
            }
            builder.append(String.format("\t%s -> %s [%s];\n", from, to, format(attributes)));
        }

        builder.append("}");
        return builder.toString();
    }

    private static String format(Map<String, String> attributes)
    {
        StringBuilder builder = new StringBuilder();

        boolean first = true;
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            if (!first) {
                builder.append(", ");
            }

            builder.append(String.format("%s=\"%s\"", entry.getKey(), entry.getValue()));
            first = false;
        }

        return builder.toString();
    }

    public CID getCluster(VID nodeId)
    {
        return membership.get(nodeId);
    }

    public Set<VID> getNodesInCluster(CID cluster)
    {
        return Collections.unmodifiableSet(nodesByCluster.get(cluster));
    }

    public Optional<V> getNode(VID id)
    {
        return Optional.fromNullable(nodes.get(id));
    }

    public E getEdge(VID from, VID to)
    {
        return edges.get(new Edge<>(from, to, false));
    }

    private static final class Edge<VID>
    {
        private final VID from;
        private final VID to;
        private final boolean toCluster;

        public Edge(VID from, VID to, boolean toCluster)
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

            if (toCluster != edge.toCluster) {
                return false;
            }
            if (!from.equals(edge.from)) {
                return false;
            }
            if (!to.equals(edge.to)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = from.hashCode();
            result = 31 * result + to.hashCode();
            result = 31 * result + (toCluster ? 1 : 0);
            return result;
        }
    }
}
