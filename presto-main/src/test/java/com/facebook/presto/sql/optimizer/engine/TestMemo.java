package com.facebook.presto.sql.optimizer.engine;

import com.facebook.presto.sql.optimizer.tree.Expression;
import com.facebook.presto.sql.optimizer.tree.sql.Null;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.optimizer.tree.Expressions.apply;
import static com.facebook.presto.sql.optimizer.tree.Expressions.call;
import static com.facebook.presto.sql.optimizer.tree.Expressions.fieldDereference;
import static com.facebook.presto.sql.optimizer.tree.Expressions.lambda;
import static com.facebook.presto.sql.optimizer.tree.Expressions.localReference;
import static com.facebook.presto.sql.optimizer.tree.Expressions.reference;
import static com.facebook.presto.sql.optimizer.tree.Expressions.value;

public class TestMemo
{
    @Test
    public void testReduceNestedLambda()
            throws Exception
    {
        Expression expression =
                apply(null, lambda(null, localReference(null)), lambda(null, reference(null, 1)));

        GreedyOptimizer optimizer = new GreedyOptimizer(true);
        throw new UnsupportedOperationException("not yet implemented");
//        Expression optimized = optimizer.optimize(expression);

//        System.out.println(expression);
//        System.out.println();
//        System.out.println(optimized);
    }

    @Test
    public void testReduce()
            throws Exception
    {
        Expression expression =

                call(null, "transform",
                        call(null, "transform",
                                call(null, "transform",
                                        call(null, "transform",
                                                call(null, "transform",
                                                        call(null, "array", call(null, "row", value(null, 1))),
                                                        lambda(null, call(null, "row", new Null(null)))),
                                                lambda(null, call(null, "row", fieldDereference(null, localReference(null), 0)))),
                                        lambda(null, call(null, "row", fieldDereference(null, localReference(null), 0)))),
                                lambda(null, call(null, "row", fieldDereference(null, localReference(null), 0)))),
                        lambda(null, call(null, "row", fieldDereference(null, localReference(null), 0))));

        GreedyOptimizer optimizer = new GreedyOptimizer(true);
        throw new UnsupportedOperationException("not yet implemented");
//        Expression optimized = optimizer.optimize(expression);
//
//        System.out.println(expression);
//        System.out.println();
//        System.out.println(optimized);
    }

    @Test
    public void test()
            throws Exception
    {
        Expression expression =
                call(null, "filter",
                        call(null, "get", value(null, "t")));

        process(expression);
    }

    @Test
    public void testScalar()
            throws Exception
    {
        Expression expression =
                call(null, "+",
                        call(null, "*", value(null, 1), value(null, 2)),
                        value(null, 3));

        process(expression);
    }

    @Test
    public void testLambda()
            throws Exception
    {
        Expression expression =
                call(null, "filter",
                        call(null, "null,get", value(null, "t")),
                        lambda(null, call(null, ">",
                                localReference(null),
                                call(null, "scalar",
                                        call(null, "get", value(null, "u"))))));

        process(expression);
    }

    @Test
    public void testLambda1()
            throws Exception
    {
        Expression expression = lambda(null, localReference(null));

        process(expression);
    }

    private void process(Expression expression)
    {
        HeuristicPlannerMemo memo = new HeuristicPlannerMemo(expression);

        System.out.println(expression.toString());
        System.out.println(memo.toGraphviz());
    }
}
