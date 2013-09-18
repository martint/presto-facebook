package com.facebook.presto.sql.newplanner;

public class ConstantLong
        implements RelationalExpression
{
    private long value;

    public ConstantLong(long value)
    {
        this.value = value;
    }

    @Override
    public RelationalExpression apply(RelationalExpression param)
    {
        return this;
    }

    @Override
    public String toString()
    {
        return Long.toString(value);
    }
}
