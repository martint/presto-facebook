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

public class Main4
{
    private Main4()
    {
    }

    public static void main(String[] args)
            throws InterruptedException
    {
        Memo2 memo = new Memo2();
        String root = memo.insert(
                        new Filter("a2",
                                new Filter("a3",
                                        new Get("t"))));

        memo.insert(root,
                        new Filter("a2",
                                new Filter("a3",
                                        new Get("u"))));

        memo.insert(root,
                        new Filter("b2",
                                new Filter("b3",
                                        new Get("v"))));

        System.out.println(memo.toGraphviz());
        System.out.println();

        memo.mergeInto("G0", "G6");
        memo.verify();
        System.out.println(memo.toGraphviz());

        memo.mergeInto("G1", "G4");
        memo.verify();
        System.out.println(memo.toGraphviz());

        memo.mergeInto("G3", "G0");
        memo.verify();
        System.out.println(memo.toGraphviz());
    }
}
