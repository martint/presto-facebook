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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.sql.optimizer.engine.Pattern.ANY_RECURSIVE;
import static com.google.common.base.Preconditions.checkArgument;

public class Memo
{
    private int count;
    private final Map<String, Set<Expression>> equivalenceClasses;
    private final Map<Expression, String> expressionToClass = new HashMap<>();
    private final VariableAllocator allocator = () -> {
        ++count;
        return "g" + count;
    };

    public Memo()
    {
        equivalenceClasses = new HashMap<>();
    }

    public Memo(Map<String, Set<Expression>> equivalenceClasses)
    {
        this.equivalenceClasses = equivalenceClasses;
    }

    public EquivalenceClass insert(Expression expression)
    {
        Let let = new SSA(allocator).toSsa(expression);

        for (Map.Entry<String, Expression> assignment : let.getAssignments().entrySet()) {
            String name = assignment.getKey();
            Expression value = assignment.getValue();

            add(name, value);
        }

        return new EquivalenceClass(((Reference) let.getExpression()).getName());
    }

    private void add(String name, Expression value)
    {
        equivalenceClasses.computeIfAbsent(name, n -> new HashSet<>()).add(value);
        expressionToClass.put(value, name);
    }

    public Expression addEquivalence(EquivalenceClass equivalenceClass, Expression expression)
    {
        Expression rewritten = addRecursiveAndRewrite(expression);

        String clazz = expressionToClass.get(rewritten);
        if (!equivalenceClass.getName().equals(clazz)) {
            System.out.println(String.format("Need to merge %s and %s", equivalenceClass.getName(), clazz));
            // TODO: merge classes
        }

        return rewritten;
    }

    private Expression addRecursiveAndRewrite(Expression expression)
    {
        if (expression instanceof Reference) {
            return expression;
        }

        List<Expression> arguments = expression.getArguments().stream()
                .map(this::addRecursiveAndRewrite)
                .map(e -> new Reference(getEquivalenceClass(e).getName()))
                .collect(Collectors.toList());

        Expression rewritten = expression.copyWithArguments(arguments);
        String clazz = expressionToClass.get(rewritten);
        if (clazz == null) {
            clazz = allocator.newVariable();
            add(clazz, rewritten);
        }

        return rewritten;
    }

    public boolean isOptimized(EquivalenceClass clazz, Requirements requirements)
    {
        // TODO
        return false;
    }

    public Set<Expression> getExpressions(EquivalenceClass clazz)
    {
        return equivalenceClasses.get(clazz.getName());
    }

    public EquivalenceClass getEquivalenceClass(Expression expression)
    {
        if (expression instanceof Reference) {
            return new EquivalenceClass(((Reference) expression).getName());
        }

        return new EquivalenceClass(expressionToClass.get(expression));
    }

    public Iterator<Expression> matchPattern(Pattern pattern, Expression expression)
    {
        checkArgument(pattern.equals(ANY_RECURSIVE)); // TODO

        throw new UnsupportedOperationException("not yet implemented");
    }

    public Stream<Expression> match(Expression root)
    {
        if (root instanceof Reference) {
            return equivalenceClasses.get(((Reference) root).getName()).stream().flatMap(this::match);
        }

        if (root.getArguments().isEmpty()) {
            return Stream.of(root);
        }

        if (root.getArguments().size() == 1) {
            checkArgument(root.getArguments().get(0) instanceof Reference);

            Reference argument = (Reference) root.getArguments().get(0);
            return equivalenceClasses.get(argument.getName()).stream()
                    .flatMap(this::match)
                    .map(match -> {
                        Map<String, Expression> assignments = new HashMap<>();

                        Expression result = match;
                        if (match instanceof Let) {
                            // flatten nested Lets
                            Let let = (Let) match;

                            assignments.putAll(let.getAssignments());
                            result = let.getExpression();
                        }

                        assignments.put(argument.getName(), result);

                        return new Let(assignments, root);
                    });
        }

        throw new UnsupportedOperationException("not yet implemented");
    }

    public String dump()
    {
        StringBuilder builder = new StringBuilder();

        for (Map.Entry<String, Set<Expression>> entry : equivalenceClasses.entrySet()) {
            builder.append(entry.getKey() + ": " + entry.getValue() + "\n");
        }

        return builder.toString();
    }
}
