package com.facebook.presto.sql.truffle;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;

public abstract class ExpressionNode
        extends SqlNode
{
    public abstract Object execute(VirtualFrame frame);

    public long executeLong(VirtualFrame frame)
            throws UnexpectedResultException
    {
        return SqlTypesGen.SQLTYPES.expectLong(execute(frame));
    }

    public double executeDouble(VirtualFrame frame)
            throws UnexpectedResultException
    {
        return SqlTypesGen.SQLTYPES.expectDouble(execute(frame));
    }
}
