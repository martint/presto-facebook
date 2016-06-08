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

public class Main3
{
    private Main3()
    {
    }

    public static void main(String[] args)
            throws InterruptedException
    {
        Memo2 memo = new Memo2();
        String root = memo.insert(
                new Filter("a1",
                        new Filter("a2",
                                new Filter("a3",
                                        new Get("t")))));

        memo.insert(root,
                new Filter("a1",
                        new Filter("a2",
                                new Filter("a3",
                                        new Get("u")))));

        memo.insert(root,
                new Filter("b1",
                        new Filter("b2",
                                new Filter("b3",
                                        new Get("v")))));

        System.out.println(memo.toGraphviz());
        System.out.println();

        memo.mergeInto("G1", "G9");
        System.out.println(memo.toGraphviz());

        memo.mergeInto("G2", "G6");
        System.out.println(memo.toGraphviz());

        memo.mergeInto("G0", "G4");
        System.out.println(memo.toGraphviz());
    }
}
