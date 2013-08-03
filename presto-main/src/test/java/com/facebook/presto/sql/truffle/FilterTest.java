package com.facebook.presto.sql.truffle;

import com.facebook.presto.sql.truffle.BinaryNode.AddNode;
import com.facebook.presto.sql.truffle.BinaryNodeFactory.AddNodeFactory;
import com.facebook.presto.sql.truffle.BinaryNodeFactory.DivideNodeFactory;
import com.facebook.presto.sql.truffle.BinaryNodeFactory.ModuloNodeFactory;
import com.facebook.presto.sql.truffle.BinaryNodeFactory.MultiplyNodeFactory;
import com.facebook.presto.sql.truffle.BinaryNodeFactory.SubtractNodeFactory;
import com.facebook.presto.sql.truffle.LiteralNode.DoubleLiteral;
import com.facebook.presto.sql.truffle.LiteralNode.LongLiteral;
import com.facebook.presto.sql.truffle.LiteralNode.UnknownLiteral;
import com.facebook.presto.sql.truffle.LiteralNodeFactory.DoubleLiteralFactory;
import com.facebook.presto.sql.truffle.LiteralNodeFactory.LongLiteralFactory;
import com.facebook.presto.sql.truffle.LiteralNodeFactory.UnknownLiteralFactory;
import com.google.common.collect.ImmutableList;
import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.dsl.NodeFactory;
import com.oracle.truffle.api.nodes.NodeUtil;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertNull;


public class FilterTest
{
    private final List<NodeFactory<? extends ExpressionNode>> binaryFactories = ImmutableList.<NodeFactory<? extends ExpressionNode>>of(
            AddNodeFactory.getInstance(),
            SubtractNodeFactory.getInstance(),
            MultiplyNodeFactory.getInstance(),
            DivideNodeFactory.getInstance(),
            ModuloNodeFactory.getInstance());

    private boolean debug = false;

    @Test
    public void testLongBinary()
            throws Exception
    {
        for (NodeFactory<? extends ExpressionNode> binaryFactory : binaryFactories) {
            ExpressionNode expressionNode = binaryFactory.createNode(newLongLiteral(7), newLongLiteral(13));
            doIt(expressionNode);
        }
    }

    @Test
    public void testLongAddPromote()
            throws Exception
    {
        AddNode addNode = AddNodeFactory.create(newLongLiteral(Long.MAX_VALUE), newLongLiteral(1));
        doIt(addNode);
    }

    @Test
    public void testDoubleBinary()
            throws Exception
    {
        for (NodeFactory<? extends ExpressionNode> binaryFactory : binaryFactories) {
            ExpressionNode expressionNode = binaryFactory.createNode(newDoubleLiteral(7.1), newDoubleLiteral(13.1));
            doIt(expressionNode);
        }
    }

    @Test
    public void testLongDoubleBinary()
            throws Exception
    {
        for (NodeFactory<? extends ExpressionNode> binaryFactory : binaryFactories) {
            ExpressionNode expressionNode = binaryFactory.createNode(newLongLiteral(7), newDoubleLiteral(13.1));
            doIt(expressionNode);
        }
    }

    @Test
    public void testNullBinary()
            throws Exception
    {
        for (NodeFactory<? extends ExpressionNode> binaryFactory : binaryFactories) {
            assertNull(doIt(binaryFactory.createNode(newLongLiteral(7), newUnknownLiteral())));
            assertNull(doIt(binaryFactory.createNode(newUnknownLiteral(), newLongLiteral(7))));
            assertNull(doIt(binaryFactory.createNode(newDoubleLiteral(7), newUnknownLiteral())));
            assertNull(doIt(binaryFactory.createNode(newUnknownLiteral(), newDoubleLiteral(7))));
            assertNull(doIt(binaryFactory.createNode(newUnknownLiteral(), newUnknownLiteral())));
        }
    }

    private Object doIt(ExpressionNode expressionNode)
            throws InterruptedException
    {
        System.out.println("==========================================================================================");
        Filter rootNode = new Filter(expressionNode);
        NodeUtil.printTree(System.out, rootNode);

        Object result = null;
        try {
            CallTarget function = Truffle.getRuntime().createCallTarget(rootNode);
            for (int i = 0; i < 10000; i++) {
                long start = System.nanoTime();
                result = function.call();
                long end = System.nanoTime();

                if (debug && i % 128 == 0) {
                    System.out.printf("== iteration %d: %.3f ms\n", i, (end - start) / 1.0E06);
                }
            }

        }
        finally {
            NodeUtil.printTree(System.out, rootNode);
        }
        if (debug) {
            System.out.println(result);
        }
        Thread.sleep(10);
        return result;
    }

    private LongLiteral newLongLiteral(long value)
    {
        return LongLiteralFactory.getInstance().createNode(value);
    }

    private DoubleLiteral newDoubleLiteral(double value)
    {
        return DoubleLiteralFactory.getInstance().createNode(value);
    }

    private UnknownLiteral newUnknownLiteral()
    {
        return UnknownLiteralFactory.getInstance().createNode();
    }


}
