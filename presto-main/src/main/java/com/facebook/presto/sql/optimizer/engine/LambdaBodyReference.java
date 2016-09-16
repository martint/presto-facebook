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
package com.facebook.presto.sql.optimizer.engine;

import com.facebook.presto.sql.optimizer.tree.Expression;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Synthetic expression type that represents a link between a lambda and its body.
 *
 * This class allows us to distinguish between regular reference semantics, which
 * are used to encode expressions in the memo and the special meaning given
 * to extracting the body out of a lambda
 *
 * E.g.,
 *
 * filter(table('t'), (lambda (x) (> x 1)))
 *
 * cannot be decomposed as:
 *
 * $0 = filter($1, $2)
 * $1 = table('t')
 * $2 = (lambda (x) $3)   <<== a reference here is problematic
 * $3 = (+ $4 $5)
 * $4 = x
 * $5 = 1
 *
 * So we do something like:
 *
 * $0 = filter($1, $2)
 * $1 = table('t')
 * $2 = (lambda (x) lambda-body-ref)
 *
 * lambda-body-ref := (+ $4 $5)
 * $4 = x
 * $5 = 1
 * */
final class LambdaBodyReference
    extends Expression
{
    private final String name;

    public LambdaBodyReference(String name)
    {
        requireNonNull(name, "name is null");
        this.name = name;
    }

    @Override
    public Object terms()
    {
        return "#" + name;
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
        LambdaBodyReference that = (LambdaBodyReference) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name);
    }
}
