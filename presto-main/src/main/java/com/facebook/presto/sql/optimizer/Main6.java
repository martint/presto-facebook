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
import com.facebook.presto.sql.optimizer.tree.Expression;
import com.facebook.presto.sql.optimizer.tree.Filter;
import com.facebook.presto.sql.optimizer.tree.Get;
import com.facebook.presto.sql.optimizer.tree.Project;

public class Main6
{
    private Main6()
    {
    }

    public static void main(String[] args)
            throws InterruptedException
    {
        Memo2 memo = new Memo2();

        addEquivalentExpressions(
                memo,
                new Filter("f1",
                        new Project("p1",
                                new Get("t"))),
                new Get("t"));

        System.out.println(memo.toGraphviz());
    }

    public static void addEquivalentExpressions(Memo2 memo, Expression first, Expression... rest)
    {
        String group = memo.insert(first);
        System.out.println(memo.toGraphviz());

        for (Expression expression : rest) {
            memo.insert(group, expression);
            System.out.println(memo.toGraphviz());
        }
    }
}
