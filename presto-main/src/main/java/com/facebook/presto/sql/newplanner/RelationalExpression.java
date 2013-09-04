package com.facebook.presto.sql.newplanner;

public interface RelationalExpression
{
//    public abstract RelationalType getType();
//
//    @Override
//    public abstract int hashCode();
//
//    @Override
//    public abstract boolean equals(Object obj);
//
    public RelationalExpression apply(RelationalExpression param);
}
