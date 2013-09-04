package com.facebook.presto.sql.newplanner;

public class FieldRef
        implements RelationalExpression
{
    private final RelationalExpression expression;
    private final int field;

    public FieldRef(RelationalExpression expression, int field)
    {
        this.expression = expression;
        this.field = field;
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

        FieldRef fieldRef = (FieldRef) o;

        if (field != fieldRef.field) {
            return false;
        }
        if (!expression.equals(fieldRef.expression)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = expression.hashCode();
        result = 31 * result + field;
        return result;
    }

    @Override
    public String toString()
    {
        return String.format("field(%s, %d)", expression, field);
    }

    @Override
    public RelationalExpression apply(RelationalExpression param)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
