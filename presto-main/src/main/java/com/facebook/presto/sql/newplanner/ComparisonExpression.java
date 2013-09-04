package com.facebook.presto.sql.newplanner;

public class ComparisonExpression
        implements RelationalExpression
{
    private final String operator;
    private final RelationalExpression left;
    private final RelationalExpression right;

    public ComparisonExpression(String operator, RelationalExpression left, RelationalExpression right)
    {
        this.operator = operator;
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

        ComparisonExpression that = (ComparisonExpression) o;

        if (!left.equals(that.left)) {
            return false;
        }
        if (!operator.equals(that.operator)) {
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
        int result = operator.hashCode();
        result = 31 * result + left.hashCode();
        result = 31 * result + right.hashCode();
        return result;
    }

    @Override
    public RelationalExpression apply(RelationalExpression param)
    {
        return new ComparisonExpression(operator, left.apply(param), right.apply(param));
    }
}
