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

import com.facebook.presto.sql.optimizer.old.engine.Memo;
import com.facebook.presto.sql.optimizer.old.tree.Expression;
import com.facebook.presto.sql.optimizer.old.tree.Get;

public class Main3
{
    private Main3()
    {
    }

    public static void main(String[] args)
            throws InterruptedException
    {
        Memo memo = new Memo();

//        addEquivalentExpressions(
//                memo,
//                new Filter("a1",
//                        new Filter("a2",
//                                new Filter("a3",
//                                        new Get("t")))),
//                new Filter("a1",
//                        new Filter("a2",
//                                new Filter("a3",
//                                        new Get("u")))),
//                new Filter("b1",
//                        new Filter("b2",
//                                new Filter("b3",
//                                        new Get("v")))));

        addEquivalentExpressions(
                memo,
                new Get("t"),
                new Get("u")
        );

//        addEquivalentExpressions(
//                memo,
//                new Filter("a2",
//                        new Filter("a3",
//                                new Get("u"))),
//                new Filter("b2",
//                        new Filter("b3",
//                                new Get("v"))));
//
    }

    public static void addEquivalentExpressions(Memo memo, Expression first, Expression... rest)
    {
        String group = memo.insert(first);
        System.out.println(memo.toGraphviz());

        for (Expression expression : rest) {
//            memo.insert(group, expression);
            System.out.println(memo.toGraphviz());
        }
    }
}
