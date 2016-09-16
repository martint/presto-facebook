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
package com.facebook.presto.sql.optimizer.old.rule;

import com.facebook.presto.sql.optimizer.old.engine.Lookup;
import com.facebook.presto.sql.optimizer.old.engine.Rule;
import com.facebook.presto.sql.optimizer.old.tree.Expression;
import com.facebook.presto.sql.optimizer.old.tree.Filter;
import com.facebook.presto.sql.optimizer.old.tree.Project;
import com.facebook.presto.sql.optimizer.old.tree.Scan;

import java.util.stream.Stream;

public class CombineScanFilterProject
        implements Rule
{
    @Override
    public Stream<Expression> apply(Expression expression, Lookup lookup)
    {
        return lookup.lookup(expression)
                .filter(Project.class::isInstance)
                .map(Project.class::cast)
                .flatMap(project -> lookup.lookup(project.getArguments().get(0))
                        .filter(Filter.class::isInstance)
                        .map(Filter.class::cast)
                        .flatMap(filter -> lookup.lookup(filter.getArguments().get(0))
                                .filter(Scan.class::isInstance)
                                .map(Scan.class::cast)
                                .map(scan -> process(project, filter, scan))));
    }

    private Expression process(Project project, Filter filter, Scan scan)
    {
        throw new UnsupportedOperationException("not yet implemented");
//        return new ScanFilterProject(scan.getTable(), filter.getCriteria(), project.getExpression());
    }
}
