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

import static java.util.Objects.requireNonNull;

public final class Lambda
        extends Expression
{
    private final Expression body;

    public static Lambda lambda(Expression body)
    {
        return new Lambda(body);
    }

    public Lambda(Expression body)
    {
        requireNonNull(body, "body is null");

        this.body = body;
    }

    public Expression getBody()
    {
        return body;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(body);
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
        return Objects.equals(body, lambda.body);
    }

    @Override
    public List<Object> terms()
    {
        return ImmutableList.of("lambda", body.terms());
    }
}
