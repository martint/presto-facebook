package com.facebook.presto.sql.truffle;

import com.oracle.truffle.api.ExactMath;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.NodeChildren;
import com.oracle.truffle.api.dsl.Specialization;

@NodeChildren({@NodeChild("leftNode"), @NodeChild("rightNode")})
public abstract class BinaryNode
        extends ExpressionNode
{
    @Specialization(order = 100)
    public Unknown unknownAndAnything(Unknown left, Object right)
    {
        return null;
    }

    @Specialization(order = 101)
    public Unknown anythingAndUnknown(Object left, Unknown right)
    {
        return null;
    }

    @Specialization(order = 102)
    public Unknown unknownAndUnknown(Unknown left, Unknown right)
    {
        return null;
    }

    public abstract static class AddNode
            extends com.facebook.presto.sql.truffle.BinaryNode
    {
        @Specialization(order = 1, rewriteOn = ArithmeticException.class)
        public long addLong(long left, long right)
        {
            return ExactMath.addExact(left, right);
        }

        @Specialization(order = 2)
        public double addDouble(double left, double right)
        {
            return left + right;
        }
    }

    public abstract static class SubtractNode
            extends com.facebook.presto.sql.truffle.BinaryNode
    {
        @Specialization(order = 1, rewriteOn = ArithmeticException.class)
        public long subtractLong(long left, long right)
        {
            return ExactMath.subtractExact(left, right);
        }

        @Specialization(order = 2)
        public double subtractDouble(double left, double right)
        {
            return left - right;
        }
    }

    public abstract static class MultiplyNode
            extends com.facebook.presto.sql.truffle.BinaryNode
    {
        @Specialization(order = 1, rewriteOn = ArithmeticException.class)
        public long multiplyLong(long left, long right)
        {
            return ExactMath.multiplyExact(left, right);
        }

        @Specialization(order = 2)
        public double multiplyDouble(double left, double right)
        {
            return left * right;
        }
    }

    public abstract static class DivideNode
            extends com.facebook.presto.sql.truffle.BinaryNode
    {
        @Specialization(order = 1, rewriteOn = ArithmeticException.class)
        public long divideLong(long left, long right)
        {
            return left / right;
        }

        @Specialization(order = 2)
        public double divideDouble(double left, double right)
        {
            return left / right;
        }
    }

    public abstract static class ModuloNode
            extends com.facebook.presto.sql.truffle.BinaryNode
    {
        @Specialization(order = 1, rewriteOn = ArithmeticException.class)
        public long moduloLong(long left, long right)
        {
            return left % right;
        }

        @Specialization(order = 2)
        public double moduloDouble(double left, double right)
        {
            return left % right;
        }
    }
}
