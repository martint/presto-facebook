package com.facebook.presto.sql.truffle;

import com.oracle.truffle.api.frame.VirtualFrame;

public abstract class LiteralNode
        extends ExpressionNode
{
    public static class LongLiteral
            extends LiteralNode
    {
        private final long value;

        public LongLiteral(long value)
        {
            this.value = value;
        }

        public Object execute(VirtualFrame frame)
        {
            return value;
        }
    }

    public static class DoubleLiteral
            extends LiteralNode
    {
        private final double value;

        public DoubleLiteral(double value)
        {
            this.value = value;
        }

        public Object execute(VirtualFrame frame)
        {
            return value;
        }
    }
}
