/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.optimizer.engine;

import com.facebook.presto.sql.optimizer.tree.Expression;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.optimizer.tree.Expressions.call;
import static com.facebook.presto.sql.optimizer.tree.Expressions.lambda;
import static com.facebook.presto.sql.optimizer.tree.Expressions.localReference;
import static com.facebook.presto.sql.optimizer.tree.Expressions.value;

public class TestOptimizer
{
    @Test
    public void testTrueFilter()
            throws Exception
    {
        GreedyOptimizer optimizer = new GreedyOptimizer(true);

        Expression expression =
                call(null, "logical-filter",
                        call(null, "get", value(null, "t")),
                        lambda(
                                null,
                                value(null, true)));

        System.out.println(expression.toString());
        System.out.println();
        System.out.println(optimizer.optimize(expression).toString());
    }

    @Test
    public void testFalseFilter()
            throws Exception
    {
        GreedyOptimizer optimizer = new GreedyOptimizer(true);

        Expression expression =
                call(null, "logical-filter",
                        call(null, "get", value(null, "t")),
                        lambda(
                                null,
                                value(null, false)));

        System.out.println(expression.toString());
        System.out.println();
        System.out.println(optimizer.optimize(expression).toString());
    }

    @Test
    public void testCombineFilters()
            throws Exception
    {
        GreedyOptimizer optimizer = new GreedyOptimizer(true);

        Expression expression =
                call(null, "logical-filter",
                        call(null, "logical-filter",
                                call(null, "get", value(null, "t")),
                                lambda(
                                        null,
                                        call(null, "deref", localReference(null), value(null, "x")))),
                        lambda(
                                null,
                                call(null, "deref", localReference(null), value(null, "y"))));

        System.out.println(expression.toString());
        System.out.println();
        System.out.println(optimizer.optimize(expression).toString());
    }
//    @Test
//    public void testApply1()
//            throws Exception
//    {
//        Optimizer optimizer = new GreedyOptimizer();
//
////        Expression expression =
//        new Apply(
//                new Lambda("x", new Call("not-null", new Reference("x"))),
//                new Get("t"));
//
//        Expression expression =
//                new Apply(
//                        new Lambda("x", new EnforceScalar(new Get("u"))),
//                        new Get("t"));
//
//        System.out.println(format(expression));
//        System.out.println();
//        System.out.println(format(optimizer.optimize(expression)));
//    }

//    @Test
//    public void testSelfJoin()
//            throws Exception
//    {
//        Optimizer optimizer = new GreedyOptimizer();
//
//        Expression expression =
//                new Join(Join.Type.INNER,
//                        "f",
//                        new Get("t"),
//                        new Get("t"));
//
//        System.out.println(format(expression));
//        System.out.println(format(optimizer.optimize(expression)));
//    }

//    @Test
//    public void testPushFilterThroughProject()
//            throws Exception
//    {
//        Optimizer optimizer = new GreedyOptimizer(list(set(new PushFilterThroughProject())));
//        Expression expression =
//                new Filter(
//                        new Project("p",
//                                new Get("t")),
//                        new Lambda("r", new Constant(true)));
//
//        System.out.println(format(expression));
//        System.out.println(format(optimizer.optimize(expression)));
//    }

//    @Test
//    public void testMergeLimits()
//            throws Exception
//    {
//        Optimizer optimizer = new GreedyOptimizer(list(set(new CombineGlobalLimits())));
//        Expression expression =
//                new GlobalLimit(10,
//                        new GlobalLimit(5,
//                                new Get("t")));
//
//        System.out.println(format(expression));
//        System.out.println(format(optimizer.optimize(expression)));
//    }

//    @Test
//    public void testMergeLimits2()
//            throws Exception
//    {
//        Optimizer optimizer = new GreedyOptimizer(list(set(new CombineGlobalLimits())));
//        Expression expression =
//                new GlobalLimit(5,
//                        new GlobalLimit(10,
//                                new Get("t")));
//
//        System.out.println(format(expression));
//        System.out.println(format(optimizer.optimize(expression)));
//    }

    @Test(enabled = false)
    public void testMergeFilters()
            throws Exception
    {
//        Optimizer optimizer = new GreedyOptimizer(list(set(new CombineFilters())));
//        Expression expression =
//                new Filter("f1",
//                        new Filter("f2",
//                                new Get("t")));
//
//        System.out.println(format(expression));
//        System.out.println(format(optimizer.optimize(expression)));
    }

//    @Test
//    public void testFlattenUnion()
//            throws Exception
//    {
//        Optimizer optimizer = new GreedyOptimizer(list(set(new CombineUnions())));
//        Expression expression =
//                new Union(
//                        new Union(
//                                new Get("a"),
//                                new Get("b")),
//                        new Get("c"));
//
//        System.out.println(format(expression));
//        System.out.println(format(optimizer.optimize(expression)));
//    }

//    @Test
//    public void testPushLimitThroughUnion()
//            throws Exception
//    {
//        Optimizer optimizer = new GreedyOptimizer(list(set(new PushGlobalLimitThroughUnion())));
//        Expression expression =
//                new GlobalLimit(5,
//                        new Union(
//                                new Get("a"),
//                                new Get("b")));
//
//        System.out.println(format(expression));
//        System.out.println(format(optimizer.optimize(expression)));
//    }

//    @Test
//    public void testOrderByLimitToTopN()
//            throws Exception
//    {
//        Optimizer optimizer = new GreedyOptimizer(list(set(new OrderByLimitToTopN())));
//        Expression expression =
//                new GlobalLimit(5,
//                        new Sort("s",
//                                new Get("a")));
//
//        System.out.println(format(expression));
//        System.out.println(format(optimizer.optimize(expression)));
//    }

//    @Test
//    public void testOrderByLimitToTopN2()
//            throws Exception
//    {
//        Optimizer optimizer = new GreedyOptimizer(list(
//                set(
//                        new CombineGlobalLimits(),
//                        new CombineLocalLimits()),
//                set(new OrderByLimitToTopN())));
//
//        Expression expression =
//                new GlobalLimit(10,
//                        new GlobalLimit(5,
//                                new Sort("s",
//                                        new Get("a"))));
//
//        System.out.println(format(expression));
//        System.out.println(format(optimizer.optimize(expression)));
//    }

//    @Test
//    public void test1()
//            throws Exception
//    {
//        Optimizer optimizer = new GreedyOptimizer();
//
//        Expression expression =
//                new GlobalLimit(10,
//                        new Sort("s",
//                                new Union(
//                                        new Get("a"),
//                                        new Get("b"),
//                                        new Union(
//                                                new Get("c"),
//                                                new Get("d")))));
//
//        System.out.println(format(expression));
//        System.out.println(format(optimizer.optimize(expression)));
//    }

    @Test(enabled = false)
    public void testMergeFilterAndCrossJoin()
            throws Exception
    {
//        Optimizer optimizer = new GreedyOptimizer(list(set(new CombineFilterAndCrossJoin())));
//        Expression expression =
//                new Filter("f",
//                        new CrossJoin(
//                                new Get("a"),
//                                new Get("b")));
//        System.out.println(format(expression));
//        System.out.println(format(optimizer.optimize(expression)));
    }

    @Test(enabled = false)
    public void testApply()
            throws Exception
    {
//        Optimizer optimizer = new GreedyOptimizer();
//
//        Expression expression =
//                new Filter("f",
//                        new Apply(new Lambda("u",
//                                new EnforceScalar(
//                                        new Get("t"))),
//                                new Get("y")));
//
//        System.out.println(format(expression));
//        System.out.println(format(optimizer.optimize(expression)));
    }

    @Test(enabled = false)
    public void testComplex()
            throws Exception
    {
//        Optimizer optimizer = new GreedyOptimizer();
//
//        Expression expression =
//                new GlobalLimit(3,
//                        new Sort("s0",
//                                new Filter("f0",
//                                        new Aggregate(Aggregate.Type.SINGLE, "a1",
//                                                new GlobalLimit(10,
//                                                        new GlobalLimit(5,
//                                                                new Union(
//                                                                        new Filter("f1",
//                                                                                new Union(
//                                                                                        new Project("p1",
//                                                                                                new Get("t")
//                                                                                        ),
//                                                                                        new Get("v"))
//
//                                                                        ),
//                                                                        new Filter("f2",
//                                                                                new CrossJoin(
//                                                                                        new Get("u"),
//                                                                                        new Project("p2",
//                                                                                                new Get("t")
//                                                                                        )
//                                                                                )
//                                                                        ),
//                                                                        new Intersect(
//                                                                                new Get("w"),
//                                                                                new Get("x"),
//                                                                                new Intersect(
//                                                                                        new Get("y"),
//                                                                                        new Get("z"))
//                                                                        )
//                                                                )
//                                                        )
//                                                )
//                                        )
//                                )
//                        )
//                );
//
//        System.out.println(format(expression));
//        System.out.println(format(optimizer.optimize(expression)));
    }

//    @Test
//    public void testGreedy1()
//            throws Exception
//    {
//        Optimizer optimizer = new GreedyOptimizer(
//                list(
//                        set(
//                                new CombineGlobalLimits(),
//                                new PushGlobalLimitThroughUnion(),
//                                new PushLocalLimitThroughUnion(),
//                                new CombineUnions()
//                        )));
//
//        Expression expression =
//                new GlobalLimit(5,
//                        new Union(
//                                new Union(
//                                        new Scan("a"),
//                                        new Scan("b")),
//                                new Scan("c")));
//
//        System.out.println(expression);
//        System.out.println(optimizer.optimize(expression));
//    }

    @Test(enabled = false)
    public void testGreedyOptimizer()
            throws Exception
    {
//        Optimizer optimizer = new GreedyOptimizer();
//
////        Expression expression =
//        new GlobalLimit(3,
//                new Sort("s0",
//                        new Filter("f0",
//                                new Aggregate(Aggregate.Type.SINGLE, "a1",
//                                        new GlobalLimit(10,
//                                                new GlobalLimit(5,
//                                                        new Union(
//                                                                new Filter("f1",
//                                                                        new Union(
//                                                                                new Project("p1",
//                                                                                        new Get("t")
//                                                                                ),
//                                                                                new Get("v"))
//
//                                                                ),
//                                                                new Filter("f2",
//                                                                        new CrossJoin(
//                                                                                new Get("u"),
//                                                                                new Project("p2",
//                                                                                        new Get("t")
//                                                                                )
//                                                                        )
//                                                                ),
//                                                                new Intersect(
//                                                                        new Get("w"),
//                                                                        new Get("x"),
//                                                                        new Intersect(
//                                                                                new Get("y"),
//                                                                                new Get("z"))
//                                                                )
//                                                        )
//                                                )
//                                        )
//                                )
//                        )
//                )
//        );
//
////        Expression expression =
//        new Union(
//                new Filter("f",
//                        new Get("t")),
//                new Filter("f",
//                        new Scan("t"))
//        );
//
////        Expression expression =
//        new GlobalLimit(5,
//                new Union(
//                        new Filter("f1",
//                                new Union(
//                                        new Get("a"),
//                                        new Get("b")
//                                )
//                        ),
//                        new Get("c")
//                )
//        );

//        Expression expression =
//                new GlobalLimit(5,
//                        new Filter("f",
//                                new Sort("s",
//                                        new Project("p",
//                                                new Get("t"))
//                                )
//                        )
//                );

//        System.out.println("before: " + expression);
//        System.out.println("after:  " + optimizer.optimize(expression));

//        new CostBasedOptimizer().optimize(expression);
//        Memo memo = optimizer.optimize(expression);
//        System.out.println(memo.toGraphviz());
    }
}
