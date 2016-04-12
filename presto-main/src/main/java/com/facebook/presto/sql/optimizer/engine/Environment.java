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
package com.facebook.presto.sql.optimizer.engine;

import com.facebook.presto.sql.optimizer.tree.Expression;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class Environment
{
    private final VariableAllocator allocator;
    private final Map<String, Expression> assignments = new HashMap<>();

    public Environment()
    {
        allocator = new VariableAllocator() {
            private int count;

            @Override
            public String newName()
            {
                count++;
                return "$" + count;
            }
        };
    }

    public Environment(VariableAllocator allocator)
    {
        this.allocator = allocator;
    }

    public String newVariable()
    {
        return allocator.newName();
    }

    public void set(String variable, Expression expression)
    {
        checkArgument(!assignments.containsKey(variable));

        assignments.put(variable, expression);
    }

    public Map<String, Expression> getAssignments()
    {
        return assignments;
    }
}
