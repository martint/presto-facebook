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

import java.util.HashSet;
import java.util.Set;

public class Group
{
    private final String id;
    private final Set<Expression> referrers = new HashSet<>();
    private final Set<Expression> expressions = new HashSet<>();

    public Group(String id)
    {
        this.id = id;
    }

    public String getId()
    {
        return id;
    }

    public void add(Expression expression)
    {
        expressions.add(expression);
    }

    public void addReferrer(Expression expression)
    {
        referrers.add(expression);
    }

    public Set<Expression> getExpressions()
    {
        return expressions;
    }

    public void addExpressions(Set<Expression> expressions)
    {
        this.expressions.addAll(expressions);
    }

    public Set<Expression> getReferrers()
    {
        return referrers;
    }
}
