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
package com.facebook.presto.sql.optimizer;

import com.facebook.presto.sql.optimizer.engine.GreedyOptimizer;
import com.facebook.presto.sql.optimizer.engine.Optimizer;
import com.facebook.presto.sql.optimizer.rule.CombineFilterAndCrossJoin;
import com.facebook.presto.sql.optimizer.rule.CombineFilters;
import com.facebook.presto.sql.optimizer.rule.CombineGlobalLimits;
import com.facebook.presto.sql.optimizer.rule.CombineUnions;
import com.facebook.presto.sql.optimizer.rule.OrderByLimitToTopN;
import com.facebook.presto.sql.optimizer.rule.PushFilterThroughProject;
import com.facebook.presto.sql.optimizer.rule.PushGlobalLimitThroughUnion;
import com.facebook.presto.sql.optimizer.rule.PushLocalLimitThroughUnion;
import com.facebook.presto.sql.optimizer.tree.Aggregate;
import com.facebook.presto.sql.optimizer.tree.CrossJoin;
import com.facebook.presto.sql.optimizer.tree.Expression;
import com.facebook.presto.sql.optimizer.tree.Filter;
import com.facebook.presto.sql.optimizer.tree.Get;
import com.facebook.presto.sql.optimizer.tree.GlobalLimit;
import com.facebook.presto.sql.optimizer.tree.Intersect;
import com.facebook.presto.sql.optimizer.tree.Project;
import com.facebook.presto.sql.optimizer.tree.Scan;
import com.facebook.presto.sql.optimizer.tree.Sort;
import com.facebook.presto.sql.optimizer.tree.Union;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

public class TestOptimizer
{
    @Test
    public void testPushFilterThroughProject()
            throws Exception
    {
        Optimizer optimizer = new Optimizer(ImmutableList.of(new PushFilterThroughProject()));
        Expression expression =
                new Filter("f",
                        new Project("p",
                                new Get("t")));

        System.out.println(optimizer.optimize(expression).toGraphviz());
    }

    @Test
    public void testMergeLimits()
            throws Exception
    {
        Optimizer optimizer = new Optimizer(ImmutableList.of(new CombineGlobalLimits()));
        Expression expression =
                new GlobalLimit(10,
                        new GlobalLimit(5,
                                new Get("t")));

        System.out.println(optimizer.optimize(expression).toGraphviz());
    }

    @Test
    public void testMergeLimits2()
            throws Exception
    {
        Optimizer optimizer = new Optimizer(ImmutableList.of(new CombineGlobalLimits()));
        Expression expression =
                new GlobalLimit(5,
                        new GlobalLimit(10,
                                new Get("t")));

        System.out.println(optimizer.optimize(expression).toGraphviz());
    }

    @Test
    public void testMergeFilters()
            throws Exception
    {
        Optimizer optimizer = new Optimizer(ImmutableList.of(new CombineFilters()));
        Expression expression =
                new Filter("f1",
                        new Filter("f2",
                                new Get("t")));

        System.out.println(optimizer.optimize(expression).toGraphviz());
    }

    @Test
    public void testFlattenUnion()
            throws Exception
    {
        Optimizer optimizer = new Optimizer(ImmutableList.of(new CombineUnions()));
        Expression expression =
                new Union(
                        new Union(
                                new Get("a"),
                                new Get("b")),
                        new Get("c"));

        System.out.println(optimizer.optimize(expression).toGraphviz());
    }

    @Test
    public void testPushLimitThroughUnion()
            throws Exception
    {
        Optimizer optimizer = new Optimizer(ImmutableList.of(new PushGlobalLimitThroughUnion()));
        Expression expression =
                new GlobalLimit(5,
                        new Union(
                                new Get("a"),
                                new Get("b")));

        System.out.println(optimizer.optimize(expression).toGraphviz());
    }

    @Test
    public void testOrderByLimitToTopN()
            throws Exception
    {
        Optimizer optimizer = new Optimizer(ImmutableList.of(new OrderByLimitToTopN()));
        Expression expression =
                new GlobalLimit(5,
                        new Sort("s",
                                new Get("a")));

        System.out.println(optimizer.optimize(expression).toGraphviz());
    }

    @Test
    public void testOrderByLimitToTopN2()
            throws Exception
    {
        Optimizer optimizer = new Optimizer(ImmutableList.of(new OrderByLimitToTopN(), new CombineGlobalLimits()));
        Expression expression =
                new GlobalLimit(10,
                        new GlobalLimit(5,
                                new Sort("s",
                                        new Get("a"))));

        System.out.println(optimizer.optimize(expression).toGraphviz());
    }

    @Test
    public void testMergeFilterAndCrossJoin()
            throws Exception
    {
        Optimizer optimizer = new Optimizer(ImmutableList.of(new CombineFilterAndCrossJoin()));
        Expression expression =
                new Filter("f",
                        new CrossJoin(
                                new Get("a"),
                                new Get("b")));

        System.out.println(optimizer.optimize(expression).toGraphviz());
    }

    @Test
    public void testComplex()
            throws Exception
    {
        Optimizer optimizer = new Optimizer();

        Expression expression =
                new GlobalLimit(3,
                        new Sort("s0",
                                new Filter("f0",
                                        new Aggregate(Aggregate.Type.SINGLE, "a1",
                                                new GlobalLimit(10,
                                                        new GlobalLimit(5,
                                                                new Union(
                                                                        new Filter("f1",
                                                                                new Union(
                                                                                        new Project("p1",
                                                                                                new Get("t")
                                                                                        ),
                                                                                        new Get("v"))

                                                                        ),
                                                                        new Filter("f2",
                                                                                new CrossJoin(
                                                                                        new Get("u"),
                                                                                        new Project("p2",
                                                                                                new Get("t")
                                                                                        )
                                                                                )
                                                                        ),
                                                                        new Intersect(
                                                                                new Get("w"),
                                                                                new Get("x"),
                                                                                new Intersect(
                                                                                        new Get("y"),
                                                                                        new Get("z"))
                                                                        )
                                                                )
                                                        )
                                                )
                                        )
                                )
                        )
                );

        System.out.println(optimizer.optimize(expression).toGraphviz());
    }

    @Test
    public void testGreedy1()
            throws Exception
    {
        GreedyOptimizer optimizer = new GreedyOptimizer(
                ImmutableList.of(
                        ImmutableSet.of(
                                new CombineGlobalLimits(),
                                new PushGlobalLimitThroughUnion(),
                                new PushLocalLimitThroughUnion(),
                                new CombineUnions()
                        )));

        Expression expression =
                new GlobalLimit(5,
                        new Union(
                                new Union(
                                        new Scan("a"),
                                        new Scan("b")),
                                new Scan("c")));

        System.out.println(optimizer.optimize(expression));
    }

    @Test
    public void testGreedyOptimizer()
            throws Exception
    {
        GreedyOptimizer optimizer = new GreedyOptimizer();

//        Expression expression =
                new GlobalLimit(3,
                        new Sort("s0",
                                new Filter("f0",
                                        new Aggregate(Aggregate.Type.SINGLE, "a1",
                                                new GlobalLimit(10,
                                                        new GlobalLimit(5,
                                                                new Union(
                                                                        new Filter("f1",
                                                                                new Union(
                                                                                        new Project("p1",
                                                                                                new Get("t")
                                                                                        ),
                                                                                        new Get("v"))

                                                                        ),
                                                                        new Filter("f2",
                                                                                new CrossJoin(
                                                                                        new Get("u"),
                                                                                        new Project("p2",
                                                                                                new Get("t")
                                                                                        )
                                                                                )
                                                                        ),
                                                                        new Intersect(
                                                                                new Get("w"),
                                                                                new Get("x"),
                                                                                new Intersect(
                                                                                        new Get("y"),
                                                                                        new Get("z"))
                                                                        )
                                                                )
                                                        )
                                                )
                                        )
                                )
                        )
                );

        Expression expression = new Union(
                new Filter("f",
                        new Get("t")),
                new Filter("f",
                        new Scan("t"))
        );
//
//                new Limit(5,
//                        new Union(
//                                new Filter("f1",
//                                        new Union(
//                                                new Get("a"),
//                                                new Get("b")
//                                        )
//                                ),
//                                new Get("c")
//                        )
//                );

        System.out.println(expression);
        System.out.println(optimizer.optimize(expression));
//        Memo memo = optimizer.optimize(expression);
//        System.out.println(memo.toGraphviz());
    }
}
