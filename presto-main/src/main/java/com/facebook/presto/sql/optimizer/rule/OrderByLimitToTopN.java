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
import com.facebook.presto.sql.optimizer.tree.GlobalLimit;
import com.facebook.presto.sql.optimizer.tree.Sort;
import com.facebook.presto.sql.optimizer.tree.TopN;

import java.util.stream.Stream;

public class OrderByLimitToTopN
        implements Rule
{
    @Override
    public Stream<Expression> apply(Expression expression, Lookup lookup)
    {
        return lookup.lookup(expression)
                .filter(GlobalLimit.class::isInstance)
                .map(GlobalLimit.class::cast)
                .flatMap(parent -> lookup.lookup(parent.getArguments().get(0))
                        .filter(Sort.class::isInstance)
                        .map(Sort.class::cast)
                        .map(child -> process(parent, child)));
    }

    private Expression process(GlobalLimit parent, Sort child)
    {
        return new TopN(
                TopN.Type.GLOBAL,
                parent.getCount(),
                child.getCriteria(),
                child.getArguments().get(0));
    }
}
