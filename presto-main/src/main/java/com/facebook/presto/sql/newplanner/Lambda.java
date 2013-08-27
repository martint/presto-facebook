package com.facebook.presto.sql.newplanner;

public class Lambda
    extends RelationalExpression
{
    private final String argument;
    private final RelationalExpression body;

    public Lambda(String argument, RelationalExpression body)
    {
        this.argument = argument;
        this.body = body;
    }

//    public Lambda(String arg0, String arg1, RelationalExpression body)
//    {
//    }

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

        Lambda lambda = (Lambda) o;

        if (!argument.equals(lambda.argument)) {
            return false;
        }
        if (!body.equals(lambda.body)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = argument.hashCode();
        result = 31 * result + body.hashCode();
        return result;
    }
}
