package com.facebook.presto.sql.newplanner;

import com.facebook.presto.spi.TableHandle;

public class TableLiteral
        implements RelationalExpression
{
    private final TableHandle handle;

    public TableLiteral(TableHandle handle)
    {
        this.handle = handle;
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
        return String.format("tableRef(\"%s\")", handle);
    }

    @Override
    public RelationalExpression apply(RelationalExpression param)
    {
        return this;
    }
}
