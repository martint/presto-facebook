package com.facebook.presto.sql.newplanner;

import com.facebook.presto.spi.TableHandle;

public class TableLiteral
    extends RelationalExpression
{
    private final TableHandle handle;

    public TableLiteral(TableHandle handle)
    {
        this.handle = handle;
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

        TableLiteral that = (TableLiteral) o;

        if (!handle.equals(that.handle)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return handle.hashCode();
    }

    @Override
    public String toString()
    {
        return handle.toString();
    }
}
