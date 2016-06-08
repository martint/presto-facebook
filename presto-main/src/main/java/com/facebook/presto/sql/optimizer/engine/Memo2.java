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
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayDeque;
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

    private final Set<String> groups = new HashSet<>();
    private final Set<Expression> expressions = new HashSet<>();

    private final Map<String, Set<Expression>> expressionsByGroup = new HashMap<>();
    private final Map<Expression, String> expressionMembership = new HashMap<>();
    private final Map<String, Set<Expression>> incomingReferences = new HashMap<>();

    private final Map<Expression, Expression> rewrites = new HashMap<>();
    private final Map<String, String> merges = new HashMap<>();

    public String insert(Expression expression)
    {
        if (expression instanceof Reference) {
            String group = ((Reference) expression).getName();
            return group;
        }

        Expression rewritten = insertChildrenAndRewrite(expression);

        String group = expressionMembership.get(rewritten);
        if (group == null) {
            group = createNewGroup();
        }

        addToGroup(rewritten, group);

        return group;
    }

    public void insert(String group, Expression expression)
    {
        if (expression instanceof Reference) {
            mergeInto(group, ((Reference) expression).getName());
            return;
        }

        Expression rewritten = insertChildrenAndRewrite(expression);

        String previousGroup = expressionMembership.get(rewritten);
        if (previousGroup == null) {
            addToGroup(rewritten, group);
        }
        else if (!previousGroup.equals(group)) {
            mergeInto(previousGroup, group);
        }
    }

    private void addToGroup(Expression rewritten, String group)
    {
        expressionMembership.put(rewritten, group);
        expressions.add(rewritten);
        expressionsByGroup.get(group).add(rewritten);

        rewritten.getArguments().stream()
                .map(Reference.class::cast)
                .map(Reference::getName)
                .forEach(child -> incomingReferences.get(child).add(rewritten));
    }

    private Expression insertChildrenAndRewrite(Expression expression)
    {
        Expression rewritten = expression;

        if (!expression.getArguments().isEmpty()) {
            List<Expression> arguments = expression.getArguments().stream()
                    .map(this::insert)
                    .map(Reference::new)
                    .collect(Collectors.toList());

            rewritten = expression.copyWithArguments(arguments);
        }

        return rewritten;
    }

    private String createNewGroup()
    {
        String name = "$" + groupCounter;
        groupCounter++;

        incomingReferences.put(name, new HashSet<>());
        expressionsByGroup.put(name, new HashSet<>());
        groups.add(name);

        return name;
    }

    public void mergeInto(String targetGroup, String group)
    {
        checkArgument(groups.contains(targetGroup), "Group doesn't exist: %s", targetGroup);
        checkArgument(groups.contains(group), "Group doesn't exist: %s", group);

        verifyNoCycle(targetGroup, group);

        merges.put(group, targetGroup);

        // move all expressions to the target group
        for (Expression expression : expressionsByGroup.get(group)) {
            expressionsByGroup.get(targetGroup).add(expression);
            expressionMembership.put(expression, targetGroup);
        }
//        expressionsByGroup.get(group).clear();

        Map<String, List<Expression>> referrerGroups = incomingReferences.get(group).stream()
                .collect(Collectors.groupingBy(expressionMembership::get));

        // rewrite expressions that reference the merged group
        for (Map.Entry<String, List<Expression>> entry : referrerGroups.entrySet()) {
            for (Expression referrerExpression : entry.getValue()) {
                String referrerGroup = entry.getKey();

//                expressionsByGroup.get(referrerGroup).remove(referrerExpression);
//                if (expressionMembership.get(referrerExpression).equals(referrerGroup)) {
//                    expressionMembership.remove(referrerExpression);
//                }
//                incomingReferences.get(group).remove(referrerExpression);

                List<Expression> newArguments = referrerExpression.getArguments().stream()
                        .map(Reference.class::cast)
                        .map(Reference::getName)
                        .map(name -> name.equals(group) ? targetGroup : group)
                        .map(Reference::new)
                        .collect(Collectors.toList());

                Expression rewritten = referrerExpression.copyWithArguments(newArguments);

                insert(referrerGroup, rewritten);

                if (!rewritten.equals(referrerExpression)) {
                    rewrites.put(referrerExpression, rewritten);
                }
            }
        }

//        removeGroup(group);
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
        builder.append('\n');

        builder.append("== Rewrites ==\n");
        for (Map.Entry<Expression, Expression> entry : rewrites.entrySet()) {
            builder.append(entry.getKey() + " -> " + entry.getValue() + "\n");
        }

        return builder.toString();
    }

    private static class Node
    {
        public enum Type
        {
            GROUP, EXPRESSION
        }

        private final Type type;
        private final Object payload;
        private final boolean active;

        public Node(Type type, Object payload, boolean active)
        {
            this.active = active;
            this.type = type;
            this.payload = payload;
        }
    }

    private static class Edge
    {
        private final Type type;

        public enum Type
        {
            CONTAINS, REFERENCES, MERGED_WITH, REWRITTEN_TO
        }

        public Edge(Type type)
        {
            this.type = type;
        }
    }

    public String toGraphviz()
    {
        Map<Object, Integer> ids = new HashMap<>();
        for (String group : groups) {
            ids.put(group, ids.size());
        }
        for (Expression expression : expressions) {
            ids.put(expression, ids.size());
        }

        Graph<Integer, String, Node, Edge, Void> graph = new Graph<>();
        DisjointSets<Integer> clusters = new DisjointSets<>();
        DisjointSets<Integer> ranks = new DisjointSets<>();

        for (String group : groups) {
            clusters.add(ids.get(group));
            ranks.add(ids.get(group));

            boolean active = !merges.containsKey(group);
            graph.addNode(ids.get(group), new Node(Node.Type.GROUP, group, active));
        }

        for (Expression expression : expressions) {
            clusters.add(ids.get(expression));
            ranks.add(ids.get(expression));

            boolean active = !rewrites.containsKey(expression);
            graph.addNode(ids.get(expression), new Node(Node.Type.EXPRESSION, expression, active));
        }

        // membership
        for (Map.Entry<String, Set<Expression>> entry : expressionsByGroup.entrySet()) {
            String group = entry.getKey();
            for (Expression expression : entry.getValue()) {
                clusters.union(ids.get(group), ids.get(expression));
                graph.addEdge(ids.get(group), ids.get(expression), new Edge(Edge.Type.CONTAINS));
            }
        }

        // references
        for (Map.Entry<String, Set<Expression>> entry : incomingReferences.entrySet()) {
            String group = entry.getKey();
            for (Expression expression : entry.getValue()) {
                graph.addEdge(ids.get(expression), ids.get(group), new Edge(Edge.Type.REFERENCES));
            }
        }

        // merges
        for (Map.Entry<String, String> entry : merges.entrySet()) {
            String source = entry.getKey();
            String target = entry.getValue();

            clusters.union(ids.get(source), ids.get(target));
            ranks.union(ids.get(source), ids.get(target));

            graph.addEdge(ids.get(source), ids.get(target), new Edge(Edge.Type.MERGED_WITH));
        }

        // rewrites
        for (Map.Entry<Expression, Expression> entry : rewrites.entrySet()) {
            Expression from = entry.getKey();
            Expression to = entry.getValue();

            clusters.union(ids.get(from), ids.get(to));
            ranks.union(ids.get(from), ids.get(to));

            graph.addEdge(ids.get(from), ids.get(to), new Edge(Edge.Type.REWRITTEN_TO));
        }

        int i = 0;
        for (Set<Integer> set : clusters.sets()) {
            String clusterId = Integer.toString(i++);

            graph.addCluster(clusterId, null);
            for (Integer value : set) {
                graph.addNodeToCluster(value, clusterId);
            }
        }

        return graph.toGraphviz(
                () -> ImmutableMap.of("nodesep", "0.5"),
                (nodeId, node) -> {
                    Map<String, String> attributes = new HashMap<>();
                    attributes.put("label", node.payload.toString());

                    if (node.type == Node.Type.GROUP) {
                        attributes.put("shape", "circle");
                    }
                    else {
                        attributes.put("shape", "rectangle");
                    }

                    if (!node.active) {
                        attributes.put("fillcolor", "lightgrey");
                        attributes.put("style", "filled");
                    }

                    return attributes;
                },
                (from, to, edge) -> {
                    Map<String, String> attributes = new HashMap<>();

                    if (!graph.getNode(from).get().active || !graph.getNode(to).get().active) {
                        attributes.put("color", "lightgrey");
                    }
                    switch (edge.type) {
                        case MERGED_WITH:
                        case REWRITTEN_TO:
                            attributes.put("style", "dotted");
                            break;
                    }

                    return attributes;
                },
                (clusterId, cluster) -> graph.getNodesInCluster(clusterId).stream()
                        .map(ranks::find)
                        .distinct()
                        .map(ranks::findAll)
                        .map(set -> "{rank=same;" + Joiner.on(";").join(set) + "}")
                        .collect(Collectors.joining("\n")) + "style=dotted;");
    }
}
