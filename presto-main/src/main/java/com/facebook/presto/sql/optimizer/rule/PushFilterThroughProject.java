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
package com.facebook.presto.sql.optimizer.rule;

import com.facebook.presto.sql.optimizer.engine.Resolver;
import com.facebook.presto.sql.optimizer.engine.Pattern;
import com.facebook.presto.sql.optimizer.engine.Rule;
import com.facebook.presto.sql.optimizer.tree.Expression;
import com.facebook.presto.sql.optimizer.tree.Filter;
import com.facebook.presto.sql.optimizer.tree.Project;
import com.google.common.collect.ImmutableList;

public class PushFilterThroughProject
        extends Rule
{
    @Override
    public Pattern getPattern()
    {
        return Pattern.ANY_RECURSIVE;
    }

    @Override
    public boolean canApply(Expression match)
    {
        return true;
    }

    @Override
    public Iterable<Expression> apply(Expression match)
    {
        Resolver resolver = new Resolver(match);

        Expression root = resolver.resolve(match);
        if (root.getArguments().size() != 1) {
            return ImmutableList.of();
        }

        Expression argument = resolver.resolve(root.getArguments().get(0));

        if (root instanceof Filter && argument instanceof Project) {
            return ImmutableList.of(new Project(new Filter(((Filter) root).getCriteria(), argument.getArguments().get(0))));
        }

        return ImmutableList.of();
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }
}
