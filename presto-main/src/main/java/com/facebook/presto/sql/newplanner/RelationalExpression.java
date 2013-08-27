package com.facebook.presto.sql.newplanner;

public abstract class RelationalExpression
{
    public abstract RelationalType getType();

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);
}
