package com.facebook.presto.graph;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

public class Edge<N, E>
{
    private final N from;
    private final N to;
    private final E type;

    public Edge(N from, N to, @Nullable E type)
    {
        Preconditions.checkNotNull(from, "from is null");
        Preconditions.checkNotNull(to, "to is null");

        this.from = from;
        this.to = to;
        this.type = type;
    }

    public N getFrom()
    {
        return from;
    }

    public N getTo()
    {
        return to;
    }

    public E getType()
    {
        return type;
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

        if (!from.equals(edge.from)) {
            return false;
        }
        if (!to.equals(edge.to)) {
            return false;
        }
        if (type != null ? !type.equals(edge.type) : edge.type != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = from.hashCode();
        result = 31 * result + to.hashCode();
        result = 31 * result + (type != null ? type.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return String.format("%s ==(%s)==> %s", from, type, to);
    }

    public static <N, E> Function<Edge<N, E>, N> fromGetter()
    {
        return new Function<Edge<N, E>, N>()
        {
            @Override
            public N apply(Edge<N, E> input)
            {
                return input.getFrom();
            }
        };
    }

    public static <N, E> Function<Edge<N, E>, N> toGetter()
    {
        return new Function<Edge<N, E>, N>()
        {
            @Override
            public N apply(Edge<N, E> input)
            {
                return input.getTo();
            }
        };
    }

    public static <N, E> Function<Edge<N, E>, E> typeGetter()
    {
        return new Function<Edge<N, E>, E>()
        {
            @Override
            public E apply(Edge<N, E> input)
            {
                return input.getType();
            }
        };
    }


}
