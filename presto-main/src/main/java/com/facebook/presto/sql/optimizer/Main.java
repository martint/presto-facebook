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

import com.facebook.presto.sql.optimizer.engine.Engine;
import com.facebook.presto.sql.optimizer.engine.Rule;
import com.facebook.presto.sql.optimizer.rule.PushFilterThroughProject;
import com.facebook.presto.sql.optimizer.tree.Expression;
import com.facebook.presto.sql.optimizer.tree.Filter;
import com.facebook.presto.sql.optimizer.tree.Get;
import com.facebook.presto.sql.optimizer.tree.Project;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class Main
{
    public static void main(String[] args)
    {
        Expression expression = new Filter(new Project(new Get("t")));

        List<Rule> rules = ImmutableList.of(
                new PushFilterThroughProject()
        );

        Engine engine = new Engine(rules);

        engine.optimize(expression);
    }
}
