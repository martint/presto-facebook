package com.facebook.presto.sql.truffle;

import com.facebook.presto.sql.truffle.BinaryNode.AddNode;
import com.facebook.presto.sql.truffle.BinaryNodeFactory.AddNodeFactory;
import com.facebook.presto.sql.truffle.BinaryNodeFactory.DivideNodeFactory;
import com.facebook.presto.sql.truffle.BinaryNodeFactory.ModuloNodeFactory;
import com.facebook.presto.sql.truffle.BinaryNodeFactory.MultiplyNodeFactory;
import com.facebook.presto.sql.truffle.BinaryNodeFactory.SubtractNodeFactory;
import com.facebook.presto.sql.truffle.LiteralNode.DoubleLiteral;
import com.facebook.presto.sql.truffle.LiteralNode.LongLiteral;
import com.google.common.collect.ImmutableList;
import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.dsl.NodeFactory;
import com.oracle.truffle.api.nodes.NodeUtil;
import org.testng.annotations.Test;

import java.util.List;


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
            ExpressionNode expressionNode = binaryFactory.createNode(new LongLiteral(7), new LongLiteral(13));
            doIt(expressionNode);
        }
    }

    @Test
    public void testLongAddPromote()
            throws Exception
    {
        AddNode addNode = AddNodeFactory.create(new LongLiteral(Long.MAX_VALUE), new LongLiteral(1));
        doIt(addNode);
    }

    @Test
    public void testDoubleBinary()
            throws Exception
    {
        for (NodeFactory<? extends ExpressionNode> binaryFactory : binaryFactories) {
            ExpressionNode expressionNode = binaryFactory.createNode(new DoubleLiteral(7.1), new DoubleLiteral(13.1));
            doIt(expressionNode);
        }
    }

    @Test
    public void testLongDoubleBinary()
            throws Exception
    {
        for (NodeFactory<? extends ExpressionNode> binaryFactory : binaryFactories) {
            ExpressionNode expressionNode = binaryFactory.createNode(new LongLiteral(7), new DoubleLiteral(13.1));
            doIt(expressionNode);
        }
    }

    private void doIt(ExpressionNode expressionNode)
            throws InterruptedException
    {
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
        System.out.println(result);
        Thread.sleep(10);
    }
}
