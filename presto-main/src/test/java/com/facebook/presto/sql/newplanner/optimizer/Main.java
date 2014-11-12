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

import com.facebook.presto.sql.newplanner.expression.Utils;
import com.facebook.presto.sql.newplanner.optimizer2.Optimizer2;

public class Main
{
    public static void main(String[] args)
    {
        int nodeId = 0;

        RelExpr filter = new RelExpr(nodeId++, RelExpr.Type.FILTER,
                new RelExpr(nodeId++, RelExpr.Type.PROJECT,
                        new RelExpr(nodeId++, RelExpr.Type.TABLE)));

        Optimizer2 optimizer = new Optimizer2();
        RelExpr optimized = optimizer.optimize(filter);

        dump(optimized, 0);
    }

    private static void dump(RelExpr expression, int indent)
    {
        System.out.println(Utils.indent(indent) + expression.getType());
        for (RelExpr input : expression.getInputs()) {
            dump(input, indent + 1);
        }
    }
}
