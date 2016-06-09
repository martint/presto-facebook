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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.sql.optimizer.engine.Pattern.ANY_RECURSIVE;
import static com.google.common.base.Preconditions.checkArgument;

public class Memo
{
    private final Map<String, Group> groups = new HashMap<>();
    private final Map<Expression, String> expressionToGroup = new HashMap<>();
    private final Set<String> invalidatedGroups = new HashSet<>();

    private Group root;
    private int count;
    private final VariableAllocator allocator = () -> "@" + (count++);

//    public EquivalenceClass insert(Expression expression)
//    {
//        if (root == null) {
//            root = createNewGroup();
//        }
//
//        EquivalenceClass result = new EquivalenceClass(root.getId());
//        root = insert(result, expression);
//        return result;
//    }

    private Group createNewGroup()
    {
        Group group = new Group(allocator.newName());
        groups.put(group.getId(), group);

        return group;
    }

    //     TODO: need to return rewritten expression so that explorer can queue it up for further exploration
//    public Group insert(EquivalenceClass group, Expression expression)
//    {
//        Group targetGroup = insertInternal(expression);
//
//        if (!targetGroup.getId().equals(group.getName())) {
//            return mergeGroups(targetGroup.getId(), group.getName());
//        }
//
//        return targetGroup;
//    }

    public Group mergeGroups(String a, String b)
    {
        Group newGroup = createNewGroup();

        Group group1 = groups.get(a);
        Group group2 = groups.get(b);

        newGroup.addExpressions(group1.getExpressions());
        newGroup.addExpressions(group2.getExpressions());


        Map<Expression, Expression> mapping = new HashMap<>();
        mapping.put(new Reference(group1.getId()), new Reference(newGroup.getId()));
        mapping.put(new Reference(group2.getId()), new Reference(newGroup.getId()));

        for (Expression expression : group1.getReferrers()) {
            Expression rewritten = rewrite(expression, e -> mapping.getOrDefault(e, e));
//            insert(new EquivalenceClass(group));
            System.out.println(expression + " => " + rewritten);
        }

        for (Expression expression : group2.getReferrers()) {
            Expression rewritten = rewrite(expression, e -> mapping.getOrDefault(e, e));
            System.out.println(expression + " => " + rewritten);
        }

        // TODO:
        //   - rewrite referrers with new group
        //      - use insert(group, rewritten) to trigger a cascading merge up the graph?
        //   - drop old groups
        //       - remove all expressions from expression->group map
        //       - remove group from name->group map

        return newGroup;
    }

    private Expression<?> rewrite(Expression<?> expression, Function<Expression<?>, Expression<?>> mapping)
    {
        List<Expression<?>> arguments = expression.getArguments().stream()
                .map(e -> rewrite(e, mapping))
                .collect(Collectors.toList());

        return mapping.apply(expression.copyWithArguments(arguments));
    }

    /**
     * @return the id of the new group
     */
    public String merge(String... groupIds)
    {
        Group newGroup = createNewGroup();

        Map<Expression, Expression> rewriteMap = new HashMap<>();

        Set<String> referrers = new HashSet<>();
        for (String groupId : groupIds) {
            Group group = groups.get(groupId);

            for (Expression expression : group.getExpressions()) {
                newGroup.add(expression);
                expressionToGroup.put(expression, newGroup.getId());
            }

            for (Expression expression : group.getReferrers()) {
                referrers.add(expressionToGroup.get(expression));
            }

            invalidatedGroups.add(groupId);

            rewriteMap.put(new Reference(groupId), new Reference(newGroup.getId()));


        }

        return newGroup.getId();
    }


//    public String rewriteGroup(String groupId, Function<String, String> mapping)
//    {
//        Group group = groups.get(groupId);
//        Group newGroup = createNewGroup();
//
//        for (Expression expression : group.getExpressions()) {
//            // All args are expected to be ReferencExpression
//
//            List<Expression> arguments = expression.getArguments().stream()
//                    .map(Reference.class::cast)
//                    .map(name -> new Reference(mapping.apply(name)))
//                    .collect(Collectors.toList());
//
//
//            newGroup.add(expression);
//            expressionToGroup.put(rewritten, newGroup.getId());
//            // TODO: need to update references from rewritten expression
//        }
//
//        Map<Expression, Expression> rewriteMap = new HashMap<>();
//        rewriteMap.put(new Reference(groupId), new Reference(newGroup.getId()));
//
//        Set<String> referrers = new HashSet<>();
//        for (Expression expression : group.getReferrers()) {
//            referrers.add(expressionToGroup.get(expression));
//        }
//
//        for (String referrer : referrers) {
//            rewriteGroup(referrer, rewriteMap::get);
//        }
//
//        return newGroup.getId();
//    }

//    private Group insertInternal(Expression expression)
//    {
//        Expression rewritten = expression;
//        if (!expression.getArguments().isEmpty()) {
//            List<Group> children = expression.getArguments().stream()
//                    .map(argument -> insertInternal(argument))
//                    .collect(Collectors.toList());
//
//            List<Expression> arguments = children.stream()
//                    .map(Group::getId)
//                    .map(Reference::new)
//                    .collect(Collectors.toList());
//
//            rewritten = expression.copyWithArguments(arguments);
//
//            for (Group child : children) {
//                child.addReferrer(rewritten);
//            }
//        }
//
//        String name = expressionToGroup.get(rewritten);
//        Group group = groups.get(name);
//        if (name == null) {
//            group = createNewGroup();
//            expressionToGroup.put(rewritten, group.getId());
//        }
//
//        group.add(rewritten);
//
//        return group;
//    }

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

        builder.append("== Groups ==\n");
        for (Map.Entry<String, Group> entry : groups.entrySet()) {
            builder.append(entry.getKey() + ": " + entry.getValue().getExpressions() + "\n");
        }

        builder.append("== Expressions ==\n");
        for (Map.Entry<Expression, String> entry : expressionToGroup.entrySet()) {
            builder.append(entry.getKey() + " ∈ " + entry.getValue() + "\n");
        }

        builder.append("== References ==\n");
        for (Group group : groups.values()) {
            for (Expression expression : group.getReferrers()) {
                builder.append(expression + " -> " + group.getId() + "\n");
            }
        }
        return builder.toString();
    }
}
