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

import com.facebook.presto.sql.optimizer.engine.Optimizer;
import com.facebook.presto.sql.optimizer.rule.FlattenUnion;
import com.facebook.presto.sql.optimizer.rule.MergeFilterAndCrossJoin;
import com.facebook.presto.sql.optimizer.rule.MergeFilters;
import com.facebook.presto.sql.optimizer.rule.MergeLimits;
import com.facebook.presto.sql.optimizer.rule.OrderByLimitToTopN;
import com.facebook.presto.sql.optimizer.rule.PushFilterThroughProject;
import com.facebook.presto.sql.optimizer.rule.PushLimitThroughUnion;
import com.facebook.presto.sql.optimizer.tree.CrossJoin;
import com.facebook.presto.sql.optimizer.tree.Expression;
import com.facebook.presto.sql.optimizer.tree.Filter;
import com.facebook.presto.sql.optimizer.tree.Get;
import com.facebook.presto.sql.optimizer.tree.Limit;
import com.facebook.presto.sql.optimizer.tree.Project;
import com.facebook.presto.sql.optimizer.tree.Sort;
import com.facebook.presto.sql.optimizer.tree.Union;
import com.google.common.collect.ImmutableList;
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
        Optimizer optimizer = new Optimizer(ImmutableList.of(new MergeLimits()));
        Expression expression =
                new Limit(10,
                        new Limit(5,
                                new Get("t")));

        System.out.println(optimizer.optimize(expression).toGraphviz());
    }

    @Test
    public void testMergeLimits2()
            throws Exception
    {
        Optimizer optimizer = new Optimizer(ImmutableList.of(new MergeLimits()));
        Expression expression =
                new Limit(5,
                        new Limit(10,
                                new Get("t")));

        System.out.println(optimizer.optimize(expression).toGraphviz());
    }

    @Test
    public void testMergeFilters()
            throws Exception
    {
        Optimizer optimizer = new Optimizer(ImmutableList.of(new MergeFilters()));
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
        Optimizer optimizer = new Optimizer(ImmutableList.of(new FlattenUnion()));
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
        Optimizer optimizer = new Optimizer(ImmutableList.of(new PushLimitThroughUnion()));
        Expression expression =
                new Limit(5,
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
                        new Limit(5,
                                new Sort("s",
                                        new Get("a")));

        System.out.println(optimizer.optimize(expression).toGraphviz());
    }

    @Test
    public void testOrderByLimitToTopN2()
            throws Exception
    {
        Optimizer optimizer = new Optimizer(ImmutableList.of(new OrderByLimitToTopN(), new MergeLimits()));
        Expression expression =
                new Limit(10,
                        new Limit(5,
                                new Sort("s",
                                        new Get("a"))));

        System.out.println(optimizer.optimize(expression).toGraphviz());
    }

    @Test
    public void testMergeFilterAndCrossJoin()
            throws Exception
    {
        Optimizer optimizer = new Optimizer(ImmutableList.of(new MergeFilterAndCrossJoin()));
        Expression expression =
                new Filter("f",
                        new CrossJoin(
                                new Get("a"),
                                new Get("b")));

        System.out.println(optimizer.optimize(expression).toGraphviz());
    }

}
