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
package com.facebook.presto.sql.newplanner.optimizer;

import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.newplanner.RelationalExpressionType;
import com.facebook.presto.sql.newplanner.expression.AggregationExpression;
import com.facebook.presto.sql.newplanner.expression.FilterExpression;
import com.facebook.presto.sql.newplanner.expression.ProjectExpression;
import com.facebook.presto.sql.newplanner.expression.RelationalExpression;
import com.facebook.presto.sql.newplanner.expression.TableExpression;
import com.facebook.presto.sql.planner.TestingColumnHandle;
import com.facebook.presto.sql.planner.TestingTableHandle;
import com.facebook.presto.sql.relational.Expressions;
import com.facebook.presto.sql.relational.Signatures;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;

public class Main
{
    public static void main(String[] args)
    {
        int nodeId = 0;

        RelExpr filter = new RelExpr(nodeId++, RelExpr.Type.FILTER,
                new RelExpr(nodeId++, RelExpr.Type.PROJECT,
                        new RelExpr(nodeId++, RelExpr.Type.TABLE)));

        Optimizer optimizer = new Optimizer();
        optimizer.optimize(filter);
    }
}
