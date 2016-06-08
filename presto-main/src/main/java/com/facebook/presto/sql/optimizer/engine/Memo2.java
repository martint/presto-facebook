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
import com.facebook.presto.sql.optimizer.tree.Reference;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class Memo2
{
    private long groupCounter;

    private final Map<String, Set<Expression>> expressionsByGroup = new HashMap<>();
    private final Map<Expression, String> expressionMembership = new HashMap<>();
    private final Map<String, Set<Expression>> incomingReferences = new HashMap<>();

    public String insert(Expression expression)
    {
        if (expression instanceof Reference) {
            String group = ((Reference) expression).getName();
            return group;
        }

        Expression rewritten = expression;
        List<String> childGroups = new ArrayList<>();
        if (!expression.getArguments().isEmpty()) {
            childGroups = expression.getArguments().stream()
                    .map(argument -> insert(argument))
                    .collect(Collectors.toList());

            List<Expression> arguments = childGroups.stream()
                    .map(Reference::new)
                    .collect(Collectors.toList());

            rewritten = expression.copyWithArguments(arguments);
        }

        String group = expressionMembership.get(rewritten);
        if (group == null) {
            group = createNewGroup();
            expressionMembership.put(rewritten, group);
        }

        expressionsByGroup.get(group).add(rewritten);

        for (String child : childGroups) {
            incomingReferences.get(child).add(rewritten);
        }

        return group;
    }

    public void insert(String group, Expression expression)
    {
        String actualGroup = insert(expression);

        if (!actualGroup.equals(group)) {
            mergeInto(group, actualGroup);
        }
    }

    private String createNewGroup()
    {
        String result = "G" + groupCounter;
        groupCounter++;

        incomingReferences.put(result, new HashSet<>());
        expressionsByGroup.put(result, new HashSet<>());

        return result;
    }

    public void mergeInto(String targetGroup, String group)
    {
        verifyNoCycle(targetGroup, group);

        // move all expressions to the target group
        for (Expression expression : expressionsByGroup.get(group)) {
            expressionsByGroup.get(targetGroup).add(expression);
            expressionMembership.put(expression, targetGroup);
        }
        expressionsByGroup.get(group).clear();

        Map<String, List<Expression>> referrersByGroup = incomingReferences.get(group).stream()
                .collect(Collectors.groupingBy(expressionMembership::get));

        for (Map.Entry<String, List<Expression>> entry : referrersByGroup.entrySet()) {
            for (Expression expression : entry.getValue()) {
                String referrer = entry.getKey();

                expressionsByGroup.get(referrer).remove(expression);
                if (expressionMembership.get(expression).equals(referrer)) {
                    expressionMembership.remove(expression);
                }
                incomingReferences.get(group).remove(expression);

                List<Expression> newArguments = expression.getArguments().stream()
                        .map(Reference.class::cast)
                        .map(Reference::getName)
                        .map(name -> name.equals(group) ? targetGroup : group)
                        .map(Reference::new)
                        .collect(Collectors.toList());

                insert(referrer, expression.copyWithArguments(newArguments));
            }
        }

        removeGroup(group);
    }

    private void verifyNoCycle(String group1, String group2)
    {
        Queue<String> pending = new ArrayDeque<>();
        pending.add(group1);

        while (!pending.isEmpty()) {
            String current = pending.poll();
            checkArgument(!current.equals(group2), "Cycle detected");

            incomingReferences.get(current).stream()
                    .map(expressionMembership::get)
                    .forEach(pending::add);
        }
    }

    private void removeGroup(String group)
    {
        checkState(expressionsByGroup.get(group).isEmpty(), "Can't remove non-empty group");

        expressionsByGroup.remove(group);
        incomingReferences.remove(group);
    }

    public void verify()
    {
        for (Map.Entry<String, Set<Expression>> entry : expressionsByGroup.entrySet()) {
            String group = entry.getKey();

            checkState(incomingReferences.containsKey(group), "Group in expressionsByGroup but not in incomingReferences: %s", group);

            for (Expression expression : entry.getValue()) {
                checkState(expressionMembership.get(expression).equals(group), "Membership for expression doesn't match group that contains it: %s, %s vs %s",
                        expression,
                        expressionMembership.get(expression),
                        group);
            }
        }

        for (Map.Entry<Expression, String> entry : expressionMembership.entrySet()) {
            Expression expression = entry.getKey();
            String group = entry.getValue();

            checkState(expressionsByGroup.containsKey(group), "Group in expressionMembership but not in expressionsByGroup: %s", group);
            checkState(expressionsByGroup.get(group).contains(expression), "expressionsByGroup does not contain expression declared by expressionMembership: %s, %s", group, expression);
        }

        for (Map.Entry<String, Set<Expression>> entry : incomingReferences.entrySet()) {
            String group = entry.getKey();
            checkState(expressionsByGroup.containsKey(group), "Group exists in incomingReferences but not in expressionsByGroup: %s", group);

            for (Expression expression : entry.getValue()) {
                checkState(expressionMembership.containsKey(expression), "Expression in incomingReferences for group %s but not in expressionMembership: %s", group, expression);
            }
        }
    }

    public String dump()
    {
        StringBuilder builder = new StringBuilder();

        builder.append("== Groups ==\n");
        for (Map.Entry<String, Set<Expression>> entry : expressionsByGroup.entrySet()) {
            builder.append(entry.getKey() + ": " + entry.getValue() + "\n");
        }
        builder.append('\n');

        builder.append("== Expressions ==\n");
        for (Map.Entry<Expression, String> entry : expressionMembership.entrySet()) {
            builder.append(entry.getKey() + " ∈ " + entry.getValue() + "\n");
        }
        builder.append('\n');

        builder.append("== References ==\n");
        for (Map.Entry<String, Set<Expression>> entry : incomingReferences.entrySet()) {
            for (Expression expression : entry.getValue()) {
                builder.append(expression + " -> " + entry.getKey() + "\n");
            }
        }
        return builder.toString();
    }

    public String toGraphviz()
    {
        StringBuilder builder = new StringBuilder();

        Set<String> activeGroups = expressionMembership.values().stream()
                .collect(Collectors.toSet());

        builder.append("digraph memo {\n");

        for (Map.Entry<String, Set<Expression>> entry : expressionsByGroup.entrySet()) {
            builder.append("\t");

            String group = entry.getKey();
            builder.append(String.format("group_%s[label=\"%s\", shape=rect];\n", group, group));

            if (!activeGroups.contains(group)) {
                for (Expression expression : entry.getValue()) {
                    // TODO: dotted line?
                    builder.append("\t");
                    builder.append(String.format("group_%s -> expression_%s;\n", group, expression.hashCode()));
                }
            }
        }

        for (Map.Entry<Expression, String> entry : expressionMembership.entrySet()) {
            Expression expression = entry.getKey();
            String group = entry.getValue();

            builder.append("\t");
            builder.append(String.format("expression_%s[label=\"%s\"];\n", expression.hashCode(), expression.toString()));

            builder.append("\t");
            builder.append(String.format("group_%s -> expression_%s;\n", group, expression.hashCode()));

//            for (Expression argument : expression.getArguments()) {
//                Reference reference = (Reference) argument;
//                builder.append("\t");
//                builder.append(String.format("expression_%s -> group_%s;\n", expression.hashCode(), reference.getName()));
//            }
        }

        for (Map.Entry<String, Set<Expression>> entry : incomingReferences.entrySet()) {
            for (Expression expression : entry.getValue()) {
                builder.append("\t");
                builder.append(String.format("expression_%s -> group_%s;\n", expression.hashCode(), entry.getKey()));
            }
        }
        builder.append("}\n");

        return builder.toString();
    }
}
