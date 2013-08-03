package com.facebook.presto.sql.truffle;

import com.oracle.truffle.api.dsl.Specialization;

public abstract class LiteralNode
        extends ExpressionNode
{
    public abstract static class LongLiteral
            extends LiteralNode
    {
        private final long value;

        public LongLiteral(long value)
        {
            this.value = value;
        }

        @Specialization
        public long getValue()
        {
            return value;
        }
    }

    public abstract static class DoubleLiteral
            extends LiteralNode
    {
        private final double value;

        public DoubleLiteral(double value)
        {
            this.value = value;
        }

        @Specialization
        public double getValue()
        {
            return value;
        }
    }

    public abstract static class UnknownLiteral
            extends LiteralNode
    {
        @Specialization
        public Object getValue()
        {
            return null;
        }
    }
}
