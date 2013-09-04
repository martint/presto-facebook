package com.facebook.presto.sql.newplanner;


public class VariableRef
    implements RelationalExpression
{
    private final String name;

    public VariableRef(String name)
    {
        this.name = name;
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

        VariableRef that = (VariableRef) o;

        if (!name.equals(that.name)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    @Override
    public String toString()
    {
        return name;
    }

    @Override
    public RelationalExpression apply(RelationalExpression param)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
