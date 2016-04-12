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
    private final Map<String, Group> groups = new HashMap<>();
    private final Map<Expression, String> expressionToGroup = new HashMap<>();

    private int count;
    private final VariableAllocator allocator = () -> "$" + (count++);

    public EquivalenceClass insert(Expression expression)
    {
        Group group = insertInternal(expression);
        return new EquivalenceClass(group.getId());
    }

//        Let let = new SSA(allocator).toSsa(expression);

//        for (Map.Entry<String, Expression> assignment : let.getAssignments().entrySet()) {
//            String name = assignment.getKey();
//            Expression value = assignment.getValue();
//
////            add(name, value);
//        }

//        return new EquivalenceClass(((Reference) let.getExpression()).getName());

    // TODO: need to return rewritten expression so that explorer can queue it up for further exploration
    public void insert(EquivalenceClass group, Expression expression)
    {
        Group targetGroup = insertInternal(expression);

        if (!targetGroup.getId().equals(group.getName())) {
            mergeGroups(targetGroup.getId(), group.getName());
        }
    }

    private void mergeGroups(String a, String b)
    {
        Group newGroup = new Group(allocator.newName());
        Group group1 = groups.get(a);
        Group group2 = groups.get(b);

        newGroup.addExpressions(group1.getExpressions());
        newGroup.addExpressions(group2.getExpressions());

        // TODO:
        //   - rewrite referrers with new group
        //      - use insert(group, rewritten) to trigger a cascading merge up the graph?
        //   - drop old groups
        //       - remove all expressions from expression->group map
        //       - remove group from name->group map
    }

    private Group insertInternal(Expression expression)
    {
        Expression rewritten = expression;
        if (!expression.getArguments().isEmpty()) {
            List<Expression> rewrittenChildren = expression.getArguments().stream()
                    .map(argument -> insertInternal(argument))
                    .map(Group::getId)
                    .map(Reference::new)
                    .collect(Collectors.toList());

            rewritten = expression.copyWithArguments(rewrittenChildren);
        }

        String name = expressionToGroup.get(rewritten);
        if (name == null) {
            name = allocator.newName();
            groups.put(name, new Group(name));
        }

        Group group = groups.get(name);
        group.add(rewritten);

        return group;
    }

//    private void add(String name, Expression value)
//    {
//        equivalenceClasses.computeIfAbsent(name, n -> new HashSet<>()).add(value);
//        expressionToClass.put(value, name);
//    }

//    public Expression addEquivalence(EquivalenceClass equivalenceClass, Expression expression)
//    {
//        Expression rewritten = addRecursiveAndRewrite(expression);
//
//        String clazz = expressionToGroup.get(rewritten);
//        if (!equivalenceClass.getName().equals(clazz)) {
//            System.out.println(String.format("Need to merge %s and %s", equivalenceClass.getName(), clazz));
//            // TODO: merge classes
//        }
//
//        return rewritten;
//    }

//    private Expression addRecursiveAndRewrite(Expression expression)
//    {
//        if (expression instanceof Reference) {
//            return expression;
//        }
//
//        List<Expression> arguments = expression.getArguments().stream()
//                .map(this::addRecursiveAndRewrite)
//                .map(e -> new Reference(getEquivalenceClass(e).getName()))
//                .collect(Collectors.toList());
//
//        Expression rewritten = expression.copyWithArguments(arguments);
//        String clazz = expressionToGroup.get(rewritten);
//        if (clazz == null) {
//            clazz = allocator.newName();
//            add(clazz, rewritten);
//        }
//
//        return rewritten;
//    }

    public boolean isOptimized(EquivalenceClass clazz, Requirements requirements)
    {
        // TODO
        return false;
    }

    public Set<Expression> getExpressions(EquivalenceClass clazz)
    {
        return groups.get(clazz.getName()).getExpressions();
    }

    public EquivalenceClass getEquivalenceClass(Expression expression)
    {
        if (expression instanceof Reference) {
            return new EquivalenceClass(((Reference) expression).getName());
        }

        return new EquivalenceClass(expressionToGroup.get(expression));
    }

    public Iterator<Expression> matchPattern(Pattern pattern, Expression expression)
    {
        checkArgument(pattern.equals(ANY_RECURSIVE)); // TODO

        throw new UnsupportedOperationException("not yet implemented");
    }

    public Stream<Expression> match(Expression root)
    {
        if (root instanceof Reference) {
            return groups.get(((Reference) root).getName()).getExpressions().stream().flatMap(this::match);
        }

        if (root.getArguments().isEmpty()) {
            return Stream.of(root);
        }

        if (root.getArguments().size() == 1) {
            checkArgument(root.getArguments().get(0) instanceof Reference);

            Reference argument = (Reference) root.getArguments().get(0);
            return groups.get(argument.getName()).getExpressions().stream()
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

        for (Map.Entry<String, Group> entry : groups.entrySet()) {
            builder.append(entry.getKey() + ": " + entry.getValue().getExpressions() + "\n");
        }

        return builder.toString();
    }
}
