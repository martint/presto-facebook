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

import com.facebook.presto.sql.newplanner.expression.RelationalExpression;
import com.google.common.base.Strings;

public class ExpressionDumper
{
    public static String dump(RelationalExpression expression)
    {
        StringBuilder builder = new StringBuilder();
        dump(builder, expression, 0);
        return builder.toString();
    }

    private static void dump(StringBuilder builder, RelationalExpression expression, int indent)
    {
        builder.append(indent(indent) + expression.getClass().getSimpleName() + "\n");
        for (RelationalExpression input : expression.getInputs()) {
            dump(builder, input, indent + 1);
        }
    }

    private static String indent(int indent)
    {
        return Strings.repeat("    ", indent);
    }
}
