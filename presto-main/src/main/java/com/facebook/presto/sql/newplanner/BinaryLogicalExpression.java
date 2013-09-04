package com.facebook.presto.sql.newplanner;

public class BinaryLogicalExpression
        implements RelationalExpression
{
    private final String operation;
    private final RelationalExpression left;
    private final RelationalExpression right;

    public BinaryLogicalExpression(String operation, RelationalExpression left, RelationalExpression right)
    {
        this.operation = operation;
        this.left = left;
        this.right = right;
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

        BinaryLogicalExpression that = (BinaryLogicalExpression) o;

        if (!left.equals(that.left)) {
            return false;
        }
        if (!operation.equals(that.operation)) {
            return false;
        }
        if (!right.equals(that.right)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = operation.hashCode();
        result = 31 * result + left.hashCode();
        result = 31 * result + right.hashCode();
        return result;
    }

    @Override
    public RelationalExpression apply(RelationalExpression param)
    {
        return new BinaryLogicalExpression(operation, left.apply(param), right.apply(param));
    }
}
