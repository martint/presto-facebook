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
package com.facebook.presto.sql.optimizer.tree2;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.facebook.presto.sql.optimizer.utils.CollectionConstructors.list;
import static java.util.Objects.requireNonNull;

public class Lambda
        extends Expression
{
    private final String variable;
    private final Expression body;

    public Lambda(String variable, Expression body)
    {
        requireNonNull(variable, "variable is null");
        requireNonNull(body, "body is null");

        this.variable = variable;
        this.body = body;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(variable, body);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Lambda lambda = (Lambda) o;
        return Objects.equals(variable, lambda.variable) &&
                Objects.equals(body, lambda.body);
    }

    @Override
    public List<Object> terms()
    {
        return ImmutableList.of("lambda", list(variable), body.terms());
    }
}
