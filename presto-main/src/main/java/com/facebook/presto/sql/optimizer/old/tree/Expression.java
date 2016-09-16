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
package com.facebook.presto.sql.optimizer.old.tree;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

// TODO:
//  consider representing everything as arguments (filter expressions, sort criteria, etc)

@Immutable
public abstract class Expression
{
    private final List<Expression> arguments;
    private int hashCode;

    public Expression(List<Expression> arguments)
    {
        requireNonNull(arguments, "arguments is null");
        this.arguments = ImmutableList.copyOf(arguments);
    }

    public final List<Expression> getArguments()
    {
        return arguments;
    }

    public abstract String getName();

    public abstract Expression copyWithArguments(List<Expression> arguments);

    protected abstract int shallowHashCode();

    protected abstract boolean shallowEquals(Expression other);

    @Override
    public String toString()
    {
        return "(" + getName() + " " + Joiner.on(" ").join(arguments) + ")";
    }

    public final boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        Expression that = (Expression) other;
        return shallowEquals(that) &&
                Objects.equals(arguments, that.getArguments());
    }

    public final int hashCode()
    {
        int hash = hashCode;
        if (hash == 0) {
            hash = Objects.hash(arguments, shallowHashCode());
            hashCode = hash;
        }

        return hash;
    }
}
