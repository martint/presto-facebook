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
package com.facebook.presto.sql.optimizer.rule2;

import com.facebook.presto.sql.optimizer.engine2.Lookup;
import com.facebook.presto.sql.optimizer.engine2.Rule;
import com.facebook.presto.sql.optimizer.tree2.Call;
import com.facebook.presto.sql.optimizer.tree2.Expression;

import java.util.stream.Stream;

import static com.facebook.presto.sql.optimizer.engine2.Patterns.isCall;
import static com.facebook.presto.sql.optimizer.tree2.Call.call;

public class LogicalToPhysicalFilter
        implements Rule
{
    @Override
    public Stream<Expression> apply(Expression expression, Lookup lookup)
    {
        return lookup.resolve(expression)
                .filter(isCall("logical-filter"))
                .map(Call.class::cast)
                .map(this::process);
    }

    private Call process(Call call)
    {
        return call("physical-filter", call.getArguments().get(0), call.getArguments().get(1));
    }
}
