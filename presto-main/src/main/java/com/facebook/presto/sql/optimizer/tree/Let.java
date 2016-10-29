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
package com.facebook.presto.sql.optimizer.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public final class Let
        extends Expression
{
    private final List<Assignment> assignments;
    private final Expression body;

    Let(List<Assignment> assignments, Expression body)
    {
        super(body.type());
        requireNonNull(assignments, "assignments is null");
        requireNonNull(body, "body is null");

        this.assignments = assignments;
        this.body = body;
    }

    public List<Assignment> getAssignments()
    {
        return assignments;
    }

    public Expression getBody()
    {
        return body;
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
        Let let = (Let) o;
        return Objects.equals(assignments, let.assignments) &&
                Objects.equals(body, let.body);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(assignments, body);
    }

    @Override
    public List<Object> terms()
    {
        return ImmutableList.builder()
                .add("let")
                .add(assignments.stream().map(Assignment::terms).collect(Collectors.toList()))
                .add(body.terms())
                .build();
    }
}
