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

import com.facebook.presto.sql.optimizer.old.engine.CostBasedOptimizer;
import com.facebook.presto.sql.optimizer.old.tree.Get;
import com.facebook.presto.sql.optimizer.old.tree.GlobalLimit;
import com.facebook.presto.sql.optimizer.old.tree.Project;
import com.facebook.presto.sql.optimizer.old.tree.Union;

public class Main
{
    private Main()
    {
    }

    public static void main(String[] args)
            throws InterruptedException
    {
//        Expression root =
                new Project("id",
                        new Get("t"));

//        Expression root =
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

//        Expression root =
        new GlobalLimit(10,
                new GlobalLimit(5,
//                                new Project("p",
                        new Get("t"))
//                        )
        );

//        Expression root =
//        new Filter("f1",
//                new Filter("f2",
//                        new Get("t")
//                )
//        );

//        Expression root =
//        new Filter("f",
//                new GlobalLimit(5,
//                        new Project("p",
//                                new Get("t"))));

//        Expression root =
        new Union(
                new Get("t"),
                new Get("t")
        );

//        Expression root =
//                new Limit(10,
        new Union(
                new Union(
                        new Union(
                                new Get("a"),
                                new Get("b")
                        ),
                        new Union(
                                new Get("c"),
                                new Get("d")
                        )
                ),
                new Get("e")
        );
//        );

        CostBasedOptimizer optimizer = new CostBasedOptimizer();

//        Memo memo = optimizer.optimize(root);
//        System.out.println(memo.toGraphviz());
    }
}
