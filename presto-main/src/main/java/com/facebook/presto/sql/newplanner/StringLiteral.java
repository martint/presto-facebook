package com.facebook.presto.sql.newplanner;

import com.google.common.base.Objects;

public class StringLiteral
        implements RelationalExpression
{
    private final String value;

    public StringLiteral(String value)
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

        StringLiteral that = (StringLiteral) o;

        if (!value.equals(that.value)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return value.hashCode();
    }

    @Override
    public String toString()
    {
        return "string(\"" + value + "\")";
    }

    @Override
    public RelationalExpression apply(RelationalExpression param)
    {
        return this;
    }
}
