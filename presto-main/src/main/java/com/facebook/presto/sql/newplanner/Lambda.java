package com.facebook.presto.sql.newplanner;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class Lambda
        implements RelationalExpression
{
    private final String argument;
    private final RelationalExpression body;

    public Lambda(String argument, RelationalExpression body)
    {
        Preconditions.checkNotNull(argument, "argument is null");
        Preconditions.checkNotNull(body, "body is null");

        this.argument = argument;
        this.body = body;
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

    @Override
    public String toString()
    {
        return argument + " -> " + body.toString();
    }

    @Override
    public RelationalExpression apply(RelationalExpression param)
    {
        return body.apply(param);
    }
}
