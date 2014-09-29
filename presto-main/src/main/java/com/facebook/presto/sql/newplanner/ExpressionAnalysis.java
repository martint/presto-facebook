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

import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.newplanner.expression.RelationalExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SubqueryExpression;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExpressionAnalysis
{
    // type for each sub-expression
    private final Map<Expression, Type> types;

    private final Set<Expression> aggregations;

    // translated subqueries
    private final Map<SubqueryExpression, RelationalExpression> translations;

    // inferred coercions for each input to the given sub-expression
    private final Map<Expression, List<List<Signature>>> coercions;

    public ExpressionAnalysis(Map<Expression, Type> types, Set<Expression> aggregations, Map<SubqueryExpression, RelationalExpression> translations, Map<Expression, List<List<Signature>>> coercions)
    {
        this.types = types;
        this.aggregations = aggregations;
        this.translations = translations;
        this.coercions = coercions;
    }
}
