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

import com.facebook.presto.sql.optimizer.engine.Lookup;
import com.facebook.presto.sql.optimizer.engine.Rule;
import com.facebook.presto.sql.optimizer.tree.Expression;
import com.facebook.presto.sql.optimizer.tree.Filter;
import com.facebook.presto.sql.optimizer.tree.Project;

import java.util.stream.Stream;

public class RemoveIdentityProjection
        implements Rule
{
    @Override
    public Stream<Expression> apply(Expression expression, Lookup lookup)
    {
        return lookup.lookup(expression)
                .filter(Filter.class::isInstance)
                .map(Filter.class::cast)
                .flatMap(parent -> lookup.lookup(parent.getArguments().get(0))
                        .filter(Project.class::isInstance)
                        .map(Project.class::cast)
                        .map(child -> process(parent, child)));
    }

    private Expression process(Filter parent, Project child)
    {
        return new Project(child.getExpression(),
                new Filter(parent.getCriteria(),
                        child.getArguments().get(0)));
    }
}
