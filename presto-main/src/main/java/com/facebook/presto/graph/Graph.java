package com.facebook.presto.graph;

import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.graph.Edge.fromGetter;
import static com.facebook.presto.graph.Edge.toGetter;
import static com.google.common.base.Predicates.and;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static java.lang.String.format;

@NotThreadSafe
public class Graph<N, E>
{
    private final Map<N, Node<N, E>> nodes = new HashMap<>();

    public Graph<N, E> addEdge(N from, N to)
    {
        return addEdge(from, to, null);
    }

    public void removeEdge(N from, N to, E type)
    {
        Preconditions.checkNotNull(from, "from is null");
        Preconditions.checkNotNull(to, "to is null");

        Node<N, E> fromNode = nodes.get(from);
        Preconditions.checkArgument(fromNode != null, "from is not in the graph");

        Node<N, E> toNode = nodes.get(to);
        Preconditions.checkArgument(toNode != null, "to is not in the graph");

        fromNode.removeOutgoingEdges(to, type);
        toNode.removeIncomingEdges(from, type);
    }

    public void removeEdges(Iterable<Edge<N, E>> edges)
    {
        for (Edge<N, E> edge : edges) {
            removeEdge(edge.getFrom(), edge.getTo(), edge.getType());
        }
    }

    public Graph<N, E> addNodes(Iterable<N> nodes)
    {
        for (N node : nodes) {
            addNode(node);
        }

        return this;
    }

    public Graph<N, E> addNode(N node)
    {
        Node<N, E> fromNode = nodes.get(node);
        if (fromNode == null) {
            fromNode = Node.nodeOf(node);
            nodes.put(node, fromNode);
        }

        return this;
    }

    public void removeNode(N node)
    {
        Node<N, E> instance = nodes.get(node);
        if (instance != null) {
            for (N predecessor : instance.getPredecessors()) {
                nodes.get(predecessor).removeOutgoingEdges(node);
            }

            for (N successor : instance.getSuccessors()) {
                nodes.get(successor).removeIncomingEdges(node);
            }

            nodes.remove(node);
        }
    }

    public void removeNodes(Iterable<? extends N> nodes)
    {
        for (N node : nodes) {
            removeNode(node);
        }
    }


    public Graph<N, E> addEdge(N from, N to, E type)
    {
        Preconditions.checkNotNull(from, "from is null");
        Preconditions.checkNotNull(to, "to is null");

        Node<N, E> fromNode = nodes.get(from);
        if (fromNode == null) {
            fromNode = Node.nodeOf(from);
            nodes.put(from, fromNode);
        }

        Node<N, E> toNode = nodes.get(to);
        if (toNode == null) {
            toNode = Node.nodeOf(to);
            nodes.put(to, toNode);
        }

        fromNode.addOutgoingEdge(to, type);
        toNode.addIncomingEdge(from, type);

        return this;
    }

    public Set<N> getNodes()
    {
        return ImmutableSet.copyOf(nodes.keySet());
    }

    public Set<Edge<N, E>> getOutgoingEdges(N value)
    {
        Node<N, E> node = nodes.get(value);
        Preconditions.checkNotNull(node, "node is not in the graph");

        return ImmutableSet.copyOf(node.getOutgoingEdges());
    }

    public Set<Edge<N, E>> getIncomingEdges(N value)
    {
        Node<N, E> node = nodes.get(value);
        Preconditions.checkNotNull(node, "node is not in the graph");

        return ImmutableSet.copyOf(node.getIncomingEdges());
    }

    public void retainAll(Collection<N> nodes)
    {
        Set<N> toRemove = ImmutableSet.copyOf(Iterables.filter(this.nodes.keySet(), not(in(ImmutableSet.copyOf(nodes)))));
        for (N node : toRemove) {
            removeNode(node);
        }
    }

    public void depthFirstSearch(Collection<N> startNodes, TraversalCallback<N> callback)
    {
        int time = 0;

        Map<N, Integer> discovered = new HashMap<>();
        Set<N> finished = new HashSet<>();

        LinkedList<N> pending = new LinkedList<N>();
        pending.addAll(startNodes);

        while (!pending.isEmpty()) {
            N node = pending.peek();

            // skip nodes we have already finished
            if (finished.contains(node)) {
                pending.pop();
                continue;
            }

            // if this node hasn't been discovered, "discover" it
            if (!discovered.containsKey(node)) {
                int discoveryTime = time++;
                discovered.put(node, discoveryTime);
                callback.before(node, discovered.get(node));
            }

            // add neighbors
            boolean hasUnvisitedSuccessors = false;
            Node<N, E> nodeObject = nodes.get(node);
            Preconditions.checkArgument(nodeObject != null, "Node %s is not in graph", node);

            for (N successor : nodeObject.getSuccessors()) {
                if (!discovered.containsKey(successor)) {
                    pending.push(successor);
                    hasUnvisitedSuccessors = true;
                }
                else if (!finished.contains(successor) && discovered.get(successor) <= discovered.get(node)) {
                    callback.cycleDetected(node, successor);
                }
            }

            if (!hasUnvisitedSuccessors) {
                // if all neighbors have been visited, finish this node
                pending.pop();
                int discoveredTime = discovered.get(node);
                finished.add(node);
                callback.after(node, discoveredTime, time++);
            }
        }
    }

    public List<N> topologicalSort(N start)
    {
        final LinkedList<N> result = new LinkedList<>();

        depthFirstSearch(ImmutableList.of(start), new TraversalCallback<N>()
        {
            @Override
            public void before(N node, int discoveryTime) { }

            @Override
            public void after(N node, int discoveryTime, int visitTime)
            {
                result.addFirst(node);
            }

            @Override
            public void cycleDetected(N node, N neighbor)
            {
                throw new IllegalStateException(format("cycle detected at %s -> %s", node, neighbor));
            }
        });

        return result;
    }

    public boolean hasCycles()
    {
        final AtomicBoolean hasCycles = new AtomicBoolean(false);

        depthFirstSearch(nodes.keySet(), new TraversalCallback<N>()
        {
            @Override
            public void before(N node, int discoveryTime) { }

            @Override
            public void after(N node, int discoveryTime, int visitTime) { }

            @Override
            public void cycleDetected(N node, N neighbor)
            {
                hasCycles.set(true);
            }
        });

        return hasCycles.get();
    }

    public Set<N> getPredecessors(N value)
    {
        Node<N, E> node = nodes.get(value);
        Preconditions.checkNotNull(node, "node is not in the graph");

        return Collections.unmodifiableSet(node.getPredecessors());
    }

    public Set<N> getPredecessors(N value, E edgeType)
    {
        Node<N, E> node = nodes.get(value);
        Preconditions.checkNotNull(node, "node is not in the graph");

        return IterableTransformer.on(node.getIncomingEdges())
                .select(edgeTypeEquals(edgeType))
                .transform(Edge.<N, E>fromGetter())
                .set();
    }

    public Set<N> getSuccessors(N value)
    {
        Node<N, E> node = nodes.get(value);
        Preconditions.checkNotNull(node, "node is not in the graph");

        return Collections.unmodifiableSet(node.getSuccessors());
    }

    public N getSuccessor(N value, E edgeType)
    {
        Node<N, E> node = nodes.get(value);
        Preconditions.checkNotNull(node, "node is not in the graph");

        Edge<N, E> edge = Iterables.find(node.getOutgoingEdges(), edgeTypeEquals(edgeType), null);
        if (edge != null) {
            return edge.getTo();
        }

        return null;
    }

    public boolean hasOutgoingEdge(N value, E edgeType)
    {
        Node<N, E> node = nodes.get(value);
        Preconditions.checkNotNull(node, "node is not in the graph");

        return Iterables.any(node.getOutgoingEdges(), edgeTypeEquals(edgeType));
    }

    public boolean isReachable(N from, final N target)
    {
        final AtomicBoolean found = new AtomicBoolean(false);

        depthFirstSearch(ImmutableList.of(from), new TraversalCallback<N>()
        {
            @Override
            public void before(N node, int discoveryTime)
            {
                if (node == target) {
                    found.set(true);
                }
            }

            @Override
            public void after(N node, int discoveryTime, int visitTime) { }

            @Override
            public void cycleDetected(N node, N neighbor) { }
        });

        return found.get();
    }

    @Override
    public String toString()
    {
        return nodes.toString();
    }

    public static <N, E> Function<N, Iterable<N>> successorGetter(final Graph<N, E> graph)
    {
        return new Function<N, Iterable<N>>()
        {
            @Override
            public Iterable<N> apply(N input)
            {
                return graph.getSuccessors(input);
            }
        };
    }

    private Predicate<? super Edge<N, E>> edgeTypeEquals(final E edgeType)
    {
        return new Predicate<Edge<N, E>>()
        {
            @Override
            public boolean apply(@Nullable Edge<N, E> input)
            {
                return input.getType().equals(edgeType);
            }
        };
    }

    public boolean contains(N node)
    {
        return nodes.containsKey(node);
    }

    public int getNodeCount()
    {
        return nodes.size();
    }
}
