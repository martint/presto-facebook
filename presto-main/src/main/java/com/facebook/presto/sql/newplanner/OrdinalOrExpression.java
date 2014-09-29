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
package com.facebook.presto.sql.newplanner;

import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

/**
 * Represents an expression or an ordinal reference. The latter is used, for
 * instance, in group by or order by clauses
 */
public class OrdinalOrExpression
{
    // reference to field in underlying relation
    private final Optional<Integer> fieldIndex;
    private final Optional<Expression> expression;

    public OrdinalOrExpression(int fieldIndex)
    {
        this.fieldIndex = Optional.of(fieldIndex);
        this.expression = Optional.absent();
    }

    public OrdinalOrExpression(Expression expression)
    {
        Preconditions.checkNotNull(expression, "expression is null");

        this.fieldIndex = Optional.absent();
        this.expression = Optional.of(expression);
    }

    public boolean isOrdinal()
    {
        return fieldIndex.isPresent();
    }

    public int getOrdinal()
    {
        Preconditions.checkState(isOrdinal(), "Not an ordinal reference");
        return fieldIndex.get();
    }

    public boolean isExpression()
    {
        return expression.isPresent();
    }

    public Expression getExpression()
    {
        Preconditions.checkState(isExpression(), "Not an expression");
        return expression.get();
    }
}

