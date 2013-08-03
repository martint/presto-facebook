package com.facebook.presto.sql.truffle;

import com.oracle.truffle.api.ExactMath;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.NodeChildren;
import com.oracle.truffle.api.dsl.Specialization;

@NodeChildren({@NodeChild("leftNode"), @NodeChild("rightNode")})
public abstract class BinaryNode
        extends ExpressionNode
{
    @Specialization
    public Object genericBinaryOperation(Object left, Object right)
    {
        if (left == null || right == null) {
            return null;
        }
        throw new UnsupportedOperationException();
    }

    public abstract static class AddNode
            extends com.facebook.presto.sql.truffle.BinaryNode
    {
        @Specialization(rewriteOn = ArithmeticException.class)
        public long addLong(long left, long right)
        {
            return ExactMath.addExact(left, right);
        }

        @Specialization
        public double addDouble(double left, double right)
        {
            return left + right;
        }
    }

    public abstract static class SubtractNode
            extends com.facebook.presto.sql.truffle.BinaryNode
    {
        @Specialization(rewriteOn = ArithmeticException.class)
        public long subtractLong(long left, long right)
        {
            return ExactMath.subtractExact(left, right);
        }

        @Specialization
        public double subtractDouble(double left, double right)
        {
            return left - right;
        }
    }

    public abstract static class MultiplyNode
            extends com.facebook.presto.sql.truffle.BinaryNode
    {
        @Specialization(rewriteOn = ArithmeticException.class)
        public long multiplyLong(long left, long right)
        {
            return ExactMath.multiplyExact(left, right);
        }

        @Specialization
        public double multiplyDouble(double left, double right)
        {
            return left * right;
        }
    }

    public abstract static class DivideNode
            extends com.facebook.presto.sql.truffle.BinaryNode
    {
        @Specialization(rewriteOn = ArithmeticException.class)
        public long divideLong(long left, long right)
        {
            return left / right;
        }

        @Specialization
        public double divideDouble(double left, double right)
        {
            return left / right;
        }
    }

    public abstract static class ModuloNode
            extends com.facebook.presto.sql.truffle.BinaryNode
    {
        @Specialization(rewriteOn = ArithmeticException.class)
        public long moduloLong(long left, long right)
        {
            return left % right;
        }

        @Specialization
        public double moduloDouble(double left, double right)
        {
            return left % right;
        }
    }
}
