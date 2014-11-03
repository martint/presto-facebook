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
package com.facebook.presto.sql.newplanner.expression;

import com.facebook.presto.sql.newplanner.optimizer.ExpressionProperties;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class OptimizationRequestExpression
        extends RelationalExpression
{
    private final ExpressionProperties requirements;

    public OptimizationRequestExpression(int id, RelationalExpression expression, ExpressionProperties requirements)
    {
        super(id, expression.getType(), ImmutableList.<RelationalExpression>of());
        this.requirements = requirements;
    }

    @Override
    public RelationalExpression copyWithInputs(int id, List<RelationalExpression> inputs)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public String toStringTree(int indent)
    {
        return Utils.indent(indent) + "- optimize(" + requirements + ")\n" +
                getInputs().get(0).toStringTree(indent + 1);
    }
}
