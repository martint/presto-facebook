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
import com.facebook.presto.sql.optimizer.engine.Memo;
import com.facebook.presto.sql.optimizer.engine.Rule;
import com.facebook.presto.sql.optimizer.rule.PushFilterThroughProject;
import com.facebook.presto.sql.optimizer.tree.Call;
import com.facebook.presto.sql.optimizer.tree.Expression;
import com.facebook.presto.sql.optimizer.tree.Reference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

public class Main2
{
    public static void main(String[] args)
    {
        Set<Expression> g0 = ImmutableSet.of(
                new Call("e0", new Reference("g1")),
                new Call("e1", new Reference("g2")));
        Set<Expression> g1 = ImmutableSet.of(
                new Call("e2", new Reference("g3")),
                new Call("e3", new Reference("g4")));
        Set<Expression> g2 = ImmutableSet.of(
                new Call("e6"));
        Set<Expression> g3 = ImmutableSet.of(
                new Call("e4"),
                new Call("e7"));
        Set<Expression> g4 = ImmutableSet.of(
                new Call("e5"));

        Memo memo = new Memo(
                ImmutableMap.<String, Set<Expression>>builder()
                        .put("g0", g0)
                        .put("g1", g1)
                        .put("g2", g2)
                        .put("g3", g3)
                        .put("g4", g4)
                        .build());

        System.out.println(memo.dump());
        System.out.println();
        memo.match(new Reference("g0")).forEach(System.out::println);

        List<Rule> rules = ImmutableList.of(
                new PushFilterThroughProject()
        );

        Engine engine = new Engine(rules);

//        engine.optimize(expression);
    }
}
