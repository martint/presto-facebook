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

import com.facebook.presto.sql.optimizer.engine.Memo2;
import com.facebook.presto.sql.optimizer.tree.Filter;
import com.facebook.presto.sql.optimizer.tree.Get;

public class Main
{
    private Main()
    {
    }

    public static void main(String[] args)
            throws InterruptedException
    {
        Memo2 memo = new Memo2();
        String root = memo.insert(
                new Filter("a",
                        new Filter("b",
                                new Get("t"))));
        System.out.println(memo.toGraphviz());

        memo.insert(root,
                new Filter("c",
                        new Get("u")));

        System.out.println(memo.toGraphviz());

        memo.insert(root,
                new Filter("d",
                        new Filter("e",
                                new Get("v"))));
        System.out.println(memo.toGraphviz());

        String x = memo.insert(
                new Filter("b",
                        new Get("t")));

        String y = memo.insert(
                new Filter("e",
                        new Get("v")));

        memo.mergeInto(x, y);
//        System.out.println(memo.dump());

        System.out.println(memo.toGraphviz());
    }
}
