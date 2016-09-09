package com.facebook.presto.sql.optimizer.engine2;

import com.facebook.presto.sql.optimizer.tree2.Expression;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.optimizer.tree2.Expressions.call;
import static com.facebook.presto.sql.optimizer.tree2.Expressions.lambda;
import static com.facebook.presto.sql.optimizer.tree2.Expressions.localReference;
import static com.facebook.presto.sql.optimizer.tree2.Expressions.value;

public class TestMemo
{
    @Test
    public void test()
            throws Exception
    {
        Expression expression =
                call("filter",
                        call("get", value("t")));

        Memo memo = new Memo(true);
        memo.insert(expression);

        System.out.println(memo.toGraphviz());
    }

    @Test
    public void testScalar()
            throws Exception
    {
        Expression expression =
                call("+",
                        call("*", value(1), value(2)),
                        value(3));

        Memo memo = new Memo(true);
        memo.insert(expression);

        System.out.println(memo.toGraphviz());
    }

    @Test
    public void testLambda()
            throws Exception
    {
        Expression expression =
                call("filter",
                        call("get", value("t")),
                        lambda(call(">",
                                localReference(),
                                call("scalar",
                                        call("get", value("u"))))));

        process(expression);
    }

    @Test
    public void testLambda1()
            throws Exception
    {
        Expression expression = lambda(localReference());

        process(expression);

    }

    private void process(Expression expression)
    {
        Memo memo = new Memo(true);
        memo.insert(expression);

        System.out.println(expression.toString());
        System.out.println(memo.toGraphviz());
    }
}
