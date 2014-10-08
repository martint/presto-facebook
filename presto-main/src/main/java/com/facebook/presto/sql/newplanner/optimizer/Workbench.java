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

import com.facebook.presto.sql.newplanner.Scope;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;

public class Workbench
{
    /**
     * @param variable name of the variable bound to each row in the FROM clause
     */
    public Workbench(String variable, Scope scope)
    {
    }

    public void add(Expression expression)
    {

    }

    public FunctionCall getAggregates()
    {

    }
}
