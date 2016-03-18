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
import com.facebook.presto.sql.optimizer.tree.Let;
import com.facebook.presto.sql.optimizer.tree.Reference;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SSA
{
    private final Map<String, Expression> assignments = new HashMap<>();
    private final VariableAllocator allocator;

    public SSA(VariableAllocator allocator)
    {
        this.allocator = allocator;
    }

    public Let toSsa(Expression expression)
    {
        Reference root = assignVariable(expression);
        return new Let(assignments, root);
    }

    private Reference assignVariable(Expression expression)
    {
        String name = allocator.newVariable();

        List<Expression> arguments = expression.getArguments().stream()
                .map(this::assignVariable)
                .collect(Collectors.toList());

        assignments.put(name, expression.copyWithArguments(arguments));

        return new Reference(name);
    }
}
