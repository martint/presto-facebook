package com.facebook.presto.sql.newplanner;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class FunctionCall
        implements RelationalExpression
{
    // TODO: use "signature" instead of name
    private final String name;
    private List<RelationalExpression> arguments;

    public FunctionCall(String name, RelationalExpression... args)
    {
        this.name = name;
        this.arguments = ImmutableList.copyOf(args);
    }

    public FunctionCall(String name, List<RelationalExpression> args)
    {
        this.name = name;
        this.arguments = ImmutableList.copyOf(args);
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

        FunctionCall that = (FunctionCall) o;

        if (!arguments.equals(that.arguments)) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = name.hashCode();
        result = 31 * result + arguments.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "call(\"" + name + "\", " + Joiner.on(", ").join(arguments) + ")";
    }

    @Override
    public RelationalExpression apply(RelationalExpression param)
    {
        ImmutableList.Builder<RelationalExpression> args = ImmutableList.builder();
        for (RelationalExpression argument : arguments) {
            args.add(argument.apply(param));
        }

        return new FunctionCall(name, args.build());
    }
}
