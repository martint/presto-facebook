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

import com.facebook.presto.connector.system.SystemTableHandle;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.newplanner.RelationalExpressionType;
import com.facebook.presto.sql.newplanner.expression.FilterExpression;
import com.facebook.presto.sql.newplanner.expression.ProjectExpression;
import com.facebook.presto.sql.newplanner.expression.RelationalExpression;
import com.facebook.presto.sql.newplanner.expression.TableExpression;
import com.facebook.presto.sql.planner.TestingColumnHandle;
import com.facebook.presto.sql.planner.TestingTableHandle;
import com.facebook.presto.sql.relational.Expressions;
import com.facebook.presto.sql.relational.Signatures;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

public class Main
{
    private final static TableHandle TABLE = new TableHandle("test", new TestingTableHandle());
    private final static ColumnHandle COLUMN_A = new ColumnHandle("test", new TestingColumnHandle("a"));
    private final static ColumnHandle COLUMN_B = new ColumnHandle("test", new TestingColumnHandle("b"));

    public static void main(String[] args)
    {
        RelationalExpression table = new TableExpression(1, TABLE, ImmutableList.of(COLUMN_A, COLUMN_B), new RelationalExpressionType(ImmutableList.<Type>of(BIGINT, BIGINT)));
        RelationalExpression projection = new ProjectExpression(2, table, ImmutableList.of(
                Expressions.field(0, BIGINT), // a
                Expressions.call(Signatures.arithmeticNegationSignature(BIGINT, BIGINT), BIGINT,


        ));

        RelationalExpression expression = new FilterExpression(1, table, Expressions.)
    }
}
