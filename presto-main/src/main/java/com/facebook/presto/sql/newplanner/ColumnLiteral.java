package com.facebook.presto.sql.newplanner;

import com.facebook.presto.spi.ColumnHandle;

public class ColumnLiteral
    extends RelationalExpression
{
    private final ColumnHandle column;

    public ColumnLiteral(ColumnHandle column)
    {
        this.column = column;
    }

    @Override
    public RelationalType getType()
    {
        throw new UnsupportedOperationException("not yet implemented");
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

        ColumnLiteral that = (ColumnLiteral) o;

        if (!column.equals(that.column)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return column.hashCode();
    }

    @Override
    public String toString()
    {
        return column.toString();
    }
}
