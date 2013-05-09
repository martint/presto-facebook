package com.facebook.presto.graph;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

class Node<N, E>
{
    private final N value;

    private final Set<Edge<N, E>> outgoingEdges = new HashSet<>();
    private final Set<Edge<N, E>> incomingEdges = new HashSet<>();
    private final Set<N> successors = new HashSet<>();
    private final Set<N> predecessors = new HashSet<>();

    public static <T, E> Node<T, E> nodeOf(T value)
    {
        return new Node<>(value);
    }

    public Node(N value)
    {
        this.value = value;
    }

    public N getValue()
    {
        return value;
    }

    public void addOutgoingEdge(N successor, E type)
    {
        outgoingEdges.add(new Edge<>(value, successor, type));
        successors.add(successor);
    }

    public void addIncomingEdge(N predecessor, E type)
    {
        incomingEdges.add(new Edge<>(predecessor, value, type));
        predecessors.add(predecessor);
    }

    public Set<N> getSuccessors()
    {
        return successors;
    }

    public Set<N> getPredecessors()
    {
        return predecessors;
    }

    public Set<Edge<N, E>> getOutgoingEdges()
    {
        return outgoingEdges;
    }

    public Set<Edge<N, E>> getIncomingEdges()
    {
        return incomingEdges;
    }

    @Override
    public String toString()
    {
        return value.toString();
    }

    public void removeOutgoingEdges(N to)
    {
        Iterator<Edge<N, E>> iterator = outgoingEdges.iterator();
        while (iterator.hasNext()) {
            Edge<N, E> edge = iterator.next();
            if (edge.getTo().equals(to)) {
                iterator.remove();
            }
        }
        successors.remove(to);
    }

    public void removeOutgoingEdges(N to, E type)
    {
        Iterator<Edge<N, E>> iterator = outgoingEdges.iterator();
        while (iterator.hasNext()) {
            Edge<N, E> edge = iterator.next();
            if (edge.getTo().equals(to) && edge.getType().equals(type)) {
                iterator.remove();
            }
        }
        successors.remove(to);
    }

    public void removeIncomingEdges(N from)
    {
        Iterator<Edge<N, E>> iterator = incomingEdges.iterator();
        while (iterator.hasNext()) {
            Edge<N, E> edge = iterator.next();
            if (edge.getFrom().equals(from)) {
                iterator.remove();
            }
        }
        predecessors.remove(from);
    }

    public void removeIncomingEdges(N from, E type)
    {
        Iterator<Edge<N, E>> iterator = incomingEdges.iterator();
        while (iterator.hasNext()) {
            Edge<N, E> edge = iterator.next();
            if (edge.getFrom().equals(from) && edge.getType().equals(type)) {
                iterator.remove();
            }
        }
        predecessors.remove(from);
    }

}
