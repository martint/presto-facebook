package com.facebook.presto.sql.optimizer.engine;

import com.facebook.presto.sql.optimizer.tree.Expression;
import com.facebook.presto.sql.optimizer.tree.sql.Null;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.optimizer.tree.Expressions.call;
import static com.facebook.presto.sql.optimizer.tree.Expressions.fieldDereference;
import static com.facebook.presto.sql.optimizer.tree.Expressions.lambda;
import static com.facebook.presto.sql.optimizer.tree.Expressions.localReference;
import static com.facebook.presto.sql.optimizer.tree.Expressions.value;

public class TestMemo
{
    @Test
    public void testReduce()
            throws Exception
    {
        Expression expression =

                call("transform",
                        call("transform",
                                call("transform",
                                        call("transform",
                                                call("transform",
                                                        call("array", call("row", value(1))),
                                                        lambda(call("row", new Null()))),
                                                lambda(call("row", fieldDereference(localReference(), 0)))),
                                        lambda(call("row", fieldDereference(localReference(), 0)))),
                                lambda(call("row", fieldDereference(localReference(), 0)))),
                        lambda(call("row", fieldDereference(localReference(), 0))));

        GreedyOptimizer optimizer = new GreedyOptimizer(true);
        Expression optimized = optimizer.optimize(expression);

        System.out.println(expression);
        System.out.println();
        System.out.println(optimized);
    }

    @Test
    public void test()
            throws Exception
    {
        Expression expression =
                call("filter",
                        call("get", value("t")));

        process(expression);
    }

    @Test
    public void testScalar()
            throws Exception
    {
        Expression expression =
                call("+",
                        call("*", value(1), value(2)),
                        value(3));

        process(expression);
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
        HeuristicPlannerMemo memo = new HeuristicPlannerMemo();
        memo.insert(expression);

        System.out.println(expression.toString());
        System.out.println(memo.toGraphviz());
    }
}
