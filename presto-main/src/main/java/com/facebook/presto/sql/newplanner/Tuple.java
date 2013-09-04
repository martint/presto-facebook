package com.facebook.presto.sql.newplanner;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class Tuple
        implements RelationalExpression
{
    private final List<RelationalExpression> parts;

    public Tuple(RelationalExpression... parts)
    {
        this.parts = ImmutableList.copyOf(parts);
    }

    public Tuple(List<? extends RelationalExpression> parts)
    {
        this.parts = ImmutableList.copyOf(parts);
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

        Tuple tuple = (Tuple) o;

        if (!parts.equals(tuple.parts)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return parts.hashCode();
    }

    @Override
    public String toString()
    {
        return "tuple(" + Joiner.on(", ").join(parts) + ")";
    }

    @Override
    public RelationalExpression apply(RelationalExpression param)
    {
        ImmutableList.Builder<RelationalExpression> builder = ImmutableList.builder();
        for (RelationalExpression part : parts) {
            builder.add(part.apply(param));
        }

        return new Tuple(builder.build());
    }
}
