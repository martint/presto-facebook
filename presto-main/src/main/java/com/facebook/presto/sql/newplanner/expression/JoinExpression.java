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

import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.relational.RowExpression;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public final class JoinExpression
    extends RelationalExpression
{
    private final JoinNode.Type type;
    private final RowExpression condition;

    public JoinExpression(int id, JoinNode.Type type, RelationalExpression left, RelationalExpression right, RowExpression condition)
    {
        super(id, RelationalExpression.concat(left.getType(), right.getType()), ImmutableList.of(left, right));
        this.type = type;
        this.condition = condition;
    }

    @Override
    public RelationalExpression copyWithInputs(int id, List<RelationalExpression> inputs)
    {
        checkArgument(inputs.size() == 2, "Expected 2 inputs");
        return new JoinExpression(id, type, inputs.get(0), inputs.get(1), condition);
    }

    @Override
    public String toStringTree(int indent)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
