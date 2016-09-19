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
package com.facebook.presto.sql.optimizer.tree.sql;

import com.facebook.presto.sql.optimizer.tree.Apply;
import com.facebook.presto.sql.optimizer.tree.Expression;
import com.facebook.presto.sql.optimizer.tree.Lambda;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class PhysicalFilter
    extends Apply
{
    public PhysicalFilter(List<Expression> arguments)
    {
        super("physical-filter", validateArguments(arguments));
    }

    private static List<Expression> validateArguments(List<Expression> arguments)
    {
        checkArgument(arguments.size() == 2, "Expected 2 arguments, got %s", arguments.size());
        checkArgument(arguments.get(1) instanceof Lambda, "Expected second argument to be a lambda, got: %s", arguments.get(1));

        return arguments;
    }
}
