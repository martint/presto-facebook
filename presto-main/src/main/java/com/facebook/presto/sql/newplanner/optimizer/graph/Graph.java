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
import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

public class Graph<VID, V, E, C>
{
    private final Map<VID, V> nodes = new HashMap<>();
    private final Map<Edge<VID>, E> edges = new HashMap<>();
    private final Map<Integer, C> clusters = new HashMap<>();
    private final Map<VID, Integer> membership = new HashMap<>();

    public int getNodeCount()
    {
        return nodes.size();
    }

    public void addNode(VID id, int cluster, V value)
    {
        checkArgument(clusters.containsKey(cluster), "cluster does not exist: %s", cluster);
        checkArgument(!nodes.containsKey(id), "node already exists: %s", id);

        nodes.put(id, value);
        membership.put(id, cluster);
    }

    public void addNode(VID id, V value)
    {
        checkArgument(!nodes.containsKey(id), "node already exists: %s", id);
        nodes.put(id, value);
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

    public void addCluster(int cluster, C value)
    {
        checkArgument(!clusters.containsKey(cluster), "cluster already exists: %s", cluster);
        clusters.put(cluster, value);
    }

    public String toGraphviz(Function<V, String> nodeFormatter, Function<E, String> edgeFormatter, Function<C, String> clusterFormatter)
    {
        StringBuilder builder = new StringBuilder("digraph G {\n");
//        builder.append("\trankdir=BT;\n");
        builder.append("\tcompound=true;\n");
        builder.append("\tranksep=1.5;\n");
        builder.append("\tnode [shape=rectangle];\n");

        Set<VID> nodesWithoutCluster = new HashSet<>();
        Multimap<Integer, VID> nodesByCluster = HashMultimap.create();
        for (VID nodeId : nodes.keySet()) {
            Integer cluster = this.membership.get(nodeId);
            if (cluster != null) {
                nodesByCluster.put(cluster, nodeId);
            }
            else {
                nodesWithoutCluster.add(nodeId);
            }
        }

        for (Map.Entry<Integer, Collection<VID>> entry : nodesByCluster.asMap().entrySet()) {
            builder.append("\tsubgraph cluster_" + entry.getKey() + "{\n");
            builder.append(String.format("\t\t{rank=same; %s}\n", Joiner.on(" ").join(entry.getValue())));

            builder.append("\t\t" + clusterFormatter.apply(clusters.get(entry.getKey())) + ";\n");

            for (VID nodeId : entry.getValue()) {
                V node = nodes.get(nodeId);
                builder.append("\t\t" + nodeId + " [" + nodeFormatter.apply(node) + "];\n");
            }

            builder.append("\t}\n");
        }

        for (VID nodeId : nodesWithoutCluster) {
            V node = nodes.get(nodeId);
            builder.append("\t\t" + nodeId + " [" + nodeFormatter.apply(node) + "];\n");
        }

        for (Map.Entry<Edge<VID>, E> entry : edges.entrySet()) {
            VID from = entry.getKey().from;
            VID to = entry.getKey().to;
            if (entry.getKey().toCluster) {
                builder.append(String.format("\t%s -> %s [%s, lhead=cluster_%s];\n", from, to, edgeFormatter.apply(entry.getValue()), this.membership.get(to)));
            }
            else {
                builder.append(String.format("\t%s -> %s [%s];\n", from, to, edgeFormatter.apply(entry.getValue())));
            }
        }

        builder.append("}");
        return builder.toString();
    }

    public int getCluster(VID nodeId)
    {
        return membership.get(nodeId);
    }

    public Optional<V> getNode(VID id)
    {
        return Optional.fromNullable(nodes.get(id));
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
