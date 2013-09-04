package com.facebook.presto.sql.newplanner;

public class BigintLiteral
        implements RelationalExpression
{
    private final long value;

    public BigintLiteral(long value)
    {
        this.value = value;
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

        BigintLiteral that = (BigintLiteral) o;

        if (value != that.value) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (value ^ (value >>> 32));
    }

    @Override
    public String toString()
    {
        return Long.toString(value);
    }

    @Override
    public RelationalExpression apply(RelationalExpression param)
    {
        return this;
    }
}
