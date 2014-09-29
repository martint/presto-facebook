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
import com.google.common.collect.Iterables;

public class JoinExpression
    extends RelationalExpression
{
    private final JoinNode.Type type;
    private final RowExpression condition;

    public JoinExpression(int id, JoinNode.Type type, RelationalExpression left, RelationalExpression right, RowExpression condition)
    {
        super(id, ImmutableList.copyOf(Iterables.concat(left.getRowType(), right.getRowType())), ImmutableList.of(left, right));
        this.type = type;
        this.condition = condition;
    }
}
