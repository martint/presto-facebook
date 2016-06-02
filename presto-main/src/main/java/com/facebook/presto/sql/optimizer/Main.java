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

import com.facebook.presto.sql.optimizer.engine.EquivalenceClass;
import com.facebook.presto.sql.optimizer.engine.Memo;
import com.facebook.presto.sql.optimizer.tree.Expression;
import com.facebook.presto.sql.optimizer.tree.Filter;
import com.facebook.presto.sql.optimizer.tree.Get;
import com.facebook.presto.sql.optimizer.tree.Project;

public class Main
{
    private Main()
    {
    }

    public static void main(String[] args)
    {
        Expression expression1 = new Filter(new Project(new Get("t")));
        Expression expression2 = new Filter(new Project(new Get("u")));

        Expression expression3 = new Project(new Get("t"));
        Expression expression4 = new Project(new Get("u"));

        Memo memo = new Memo();
        System.out.println(memo.insert(expression1));
        System.out.println(memo.insert(expression2));

        EquivalenceClass group1 = memo.insert(expression3);
        memo.insert(group1, expression4);

        System.out.println(memo.dump());
//        engine.optimize(expression);
    }
}
