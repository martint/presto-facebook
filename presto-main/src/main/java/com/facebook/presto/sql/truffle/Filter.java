package com.facebook.presto.sql.truffle;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.RootNode;

import static com.google.common.base.Preconditions.checkNotNull;

public class Filter
        extends RootNode
{
    @Child
    private ExpressionNode expression;

    public Filter(ExpressionNode expression)
    {
        this.expression = adoptChild(checkNotNull(expression, "expression is null"));
    }

    @Override
    public Object execute(VirtualFrame virtualFrame)
    {
        return expression.execute(virtualFrame);
    }
}
