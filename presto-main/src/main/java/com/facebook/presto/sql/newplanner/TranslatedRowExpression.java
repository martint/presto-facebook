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

import com.facebook.presto.sql.relational.RowExpression;

import java.util.List;

public class TranslatedRowExpression
{
    private final RowExpression expression;
    private final List<ResolvedName> boundVariables;

    public TranslatedRowExpression(RowExpression expression, List<ResolvedName> boundVariables)
    {
        this.expression = expression;
        this.boundVariables = boundVariables;
    }

    public RowExpression getExpression()
    {
        return expression;
    }

    public List<ResolvedName> getBoundVariables()
    {
        return boundVariables;
    }
}
