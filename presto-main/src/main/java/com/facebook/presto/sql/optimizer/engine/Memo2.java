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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class Memo2
{
    private final boolean debug;

    private long version;
    private long groupCounter;

    private final Map<String, Long> groups = new HashMap<>();
    private final Map<Expression, Long> expressions = new HashMap<>();

    private final Map<String, Map<Expression, Long>> expressionsByGroup = new HashMap<>();
    private final Map<Expression, String> expressionMembership = new HashMap<>();
    private final Map<String, Map<Expression, Long>> incomingReferences = new HashMap<>();

    private final Map<Expression, VersionedItem<Expression>> rewrites = new HashMap<>();
    private final Map<String, VersionedItem<String>> merges = new HashMap<>();
    private final Map<Expression, Map<Expression, VersionedItem<String>>> transformations = new HashMap<>();

    public Memo2()
    {
        this(false);
    }

    public Memo2(boolean debug)
    {
        this.debug = debug;
    }

    public long getVersion()
    {
        return version;
    }

    /**
     * Allows possibly nested expressions
     */
    public String insert(Expression expression)
    {
        String result = insertRecursive(expression);
        if (debug) {
            verify();
        }
        return result;
    }

    // TODO: need to return whether the expression is new
    // TODO: maybe return Optional<Expression>
    public Expression transform(Expression from, Expression to, String reason)
    {
        checkArgument(expressionMembership.containsKey(from), "Unknown expression: %s", from);

        String group = expressionMembership.get(from);
        Expression result = insertRecursive(group, to);

        transformations.computeIfAbsent(canonicalize(from), e -> new HashMap<>())
                .computeIfAbsent(result, e -> new VersionedItem<>(reason, version++));

        if (debug) {
            verify();
        }

        return result;
    }

    public Set<Expression> getExpressions(String group)
    {
        // pick only active expressions -- TODO: need a more efficient way to do this
        return expressionsByGroup.get(group)
                .keySet().stream()
                .map(this::canonicalize)
                .collect(Collectors.toSet());
    }

    private Expression insertRecursive(String targetGroup, Expression expression)
    {
        // Make sure we use the latest version of a group, otherwise, we
        // may end up attempting to merge groups that are already merged
        targetGroup = canonicalize(targetGroup);

        if (expression instanceof Reference) {
            if (!((Reference) expression).getName().equals(targetGroup)) {
                mergeInto(targetGroup, ((Reference) expression).getName());
            }
            return new Reference(targetGroup);
        }

        Expression rewritten = insertChildrenAndRewrite(expression);

        String currentGroup = expressionMembership.get(rewritten);
        if (currentGroup == null) {
            insert(rewritten, targetGroup);
        }
        else if (!currentGroup.equals(targetGroup)) {
            mergeInto(targetGroup, currentGroup);
        }

        return rewritten;
    }

    private String insertRecursive(Expression expression)
    {
        checkArgument(!(expression instanceof Reference), "Expression cannot be a Reference: %s", expression);

        Expression rewritten = insertChildrenAndRewrite(expression);

        String group = expressionMembership.get(rewritten);
        if (group == null) {
            group = createNewGroup();
            insert(rewritten, group);
        }

        return group;
    }

    private Expression insertChildrenAndRewrite(Expression expression)
    {
        Expression rewritten = expression;

        if (!expression.getArguments().isEmpty()) {
            List<Expression> arguments = expression.getArguments().stream()
                    .map(arg -> {
                        if (arg instanceof Reference) {
                            // TODO: make sure group exists
                            return new Reference(canonicalize(((Reference) arg).getName()));
                        }

                        return new Reference(insertRecursive(arg));
                    })
                    .collect(Collectors.toList());
            rewritten = expression.copyWithArguments(arguments);
        }

        return rewritten;
    }

    private void insert(Expression rewritten, String group)
    {
        expressionMembership.put(rewritten, group);
        expressions.put(rewritten, version++);
        expressionsByGroup.get(group).put(rewritten, version++);

        rewritten.getArguments().stream()
                .map(Reference.class::cast)
                .map(Reference::getName)
                .forEach(child -> incomingReferences.get(child).putIfAbsent(rewritten, version++));
    }

    private String createNewGroup()
    {
        String name = "$" + groupCounter;
        groupCounter++;

        incomingReferences.put(name, new HashMap<>());
        expressionsByGroup.put(name, new HashMap<>());
        groups.put(name, version++);

        return name;
    }

    private String canonicalize(String group)
    {
        while (merges.containsKey(group)) {
            group = merges.get(group).getItem();
        }
        return group;
    }

    private Expression canonicalize(Expression expression)
    {
        checkArgument(expression.getArguments().stream().allMatch(Reference.class::isInstance), "Expected all arguments to be references");

        List<Expression> newArguments = expression.getArguments().stream()
                .map(Reference.class::cast)
                .map(Reference::getName)
                .map(this::canonicalize)
                .map(Reference::new)
                .collect(Collectors.toList());

        return expression.copyWithArguments(newArguments);
    }

    private void mergeInto(String targetGroup, String group)
    {
        checkArgument(groups.containsKey(targetGroup), "Group doesn't exist: %s", targetGroup);
        checkArgument(groups.containsKey(group), "Group doesn't exist: %s", group);
        checkArgument(!canonicalize(targetGroup).equals(canonicalize(group)), "Groups are already merged: %s vs %s", targetGroup, group);

        merges.put(group, new VersionedItem<>(targetGroup, version++));

        // move all expressions to the target group
        for (Expression expression : expressionsByGroup.get(group).keySet()) {
            expressionsByGroup.get(targetGroup).put(expression, version++);
            expressionMembership.put(expression, targetGroup);
        }

        // rewrite expressions that reference the merged group
        Map<String, List<Expression>> referrerGroups = incomingReferences.get(group).keySet().stream()
                .collect(Collectors.groupingBy(expressionMembership::get));

        for (Map.Entry<String, List<Expression>> entry : referrerGroups.entrySet()) {
            for (Expression referrerExpression : entry.getValue()) {
                String referrerGroup = entry.getKey();

                Expression rewritten = canonicalize(referrerExpression);

                String previousGroup = expressionMembership.get(rewritten);
                if (previousGroup == null) {
                    insert(rewritten, referrerGroup);
                }
                else if (!previousGroup.equals(referrerGroup)) {
                    mergeInto(referrerGroup, previousGroup);
                }

                if (!rewritten.equals(referrerExpression)) {
                    rewrites.put(referrerExpression, new VersionedItem<>(rewritten, version++));
                }
            }
        }
    }

    public void verify()
    {
        // ensure all active expressions "belong" to the canonical group name
        for (Map.Entry<Expression, String> entry : expressionMembership.entrySet()) {
            Expression expression = entry.getKey();
            String group = entry.getValue();

            checkState(group.equals(canonicalize(group)),
                    "Expression not marked as belonging to canonical group: %s (%s vs %s)  ", expression, group, canonicalize(group));

            expression.getArguments().stream()
                    .peek(e -> checkState((e instanceof Reference), "All expression arguments must be references: %s", expression))
                    .map(Reference.class::cast)
                    .peek(r -> checkState(r.getName().equals(canonicalize(r.getName())),
                            "Expression arguments must reference canonical groups: %s, %s vs %s", expression, r.getName(), canonicalize(r.getName())));
        }
    }

//    public void verify()
//    {
//        for (Map.Entry<String, Set<VersionedItem<Expression>>> entry : expressionsByGroup.entrySet()) {
//            String group = entry.getKey();
//
//            checkState(incomingReferences.containsKey(group), "Group in expressionsByGroup but not in incomingReferences: %s", group);
//
//            for (VersionedItem<Expression> expression : entry.getValue()) {
//                checkState(expressionMembership.get(expression).equals(group), "Membership for expression doesn't match group that contains it: %s, %s vs %s",
//                        expression,
//                        expressionMembership.get(expression),
//                        group);
//            }
//        }
//
//        for (Map.Entry<Expression, String> entry : expressionMembership.entrySet()) {
//            Expression expression = entry.getKey();
//            String group = entry.getValue();
//
//            checkState(expressionsByGroup.containsKey(group), "Group in expressionMembership but not in expressionsByGroup: %s", group);
//            checkState(expressionsByGroup.get(group).contains(expression), "expressionsByGroup does not contain expression declared by expressionMembership: %s, %s", group, expression);
//        }
//
//        for (Map.Entry<String, Set<Expression>> entry : incomingReferences.entrySet()) {
//            String group = entry.getKey();
//            checkState(expressionsByGroup.containsKey(group), "Group exists in incomingReferences but not in expressionsByGroup: %s", group);
//
//            for (Expression expression : entry.getValue()) {
//                checkState(expressionMembership.containsKey(expression), "Expression in incomingReferences for group %s but not in expressionMembership: %s", group, expression);
//            }
//        }
//    }

    public String dump()
    {
        StringBuilder builder = new StringBuilder();

        builder.append("== Groups ==\n");
        for (Map.Entry<String, Map<Expression, Long>> entry : expressionsByGroup.entrySet()) {
            builder.append(entry.getKey() + ": " + entry.getValue() + "\n");
        }
        builder.append('\n');

        builder.append("== Expressions ==\n");
        for (Map.Entry<Expression, String> entry : expressionMembership.entrySet()) {
            builder.append(entry.getKey() + " ∈ " + entry.getValue() + "\n");
        }
        builder.append('\n');

        builder.append("== References ==\n");
        for (Map.Entry<String, Map<Expression, Long>> entry : incomingReferences.entrySet()) {
            for (Map.Entry<Expression, Long> versioned : entry.getValue().entrySet()) {
                builder.append(versioned.getKey() + " -> " + entry.getKey() + " [" + versioned.getValue() + "]\n");
            }
        }
        builder.append('\n');

        builder.append("== Rewrites ==\n");
        for (Map.Entry<Expression, VersionedItem<Expression>> entry : rewrites.entrySet()) {
            builder.append(entry.getKey() + " -> " + entry.getValue().getItem() + " @" + entry.getValue().getVersion() + "\n");
        }

        return builder.toString();
    }

    public boolean contains(Expression expression)
    {
        return expressionMembership.containsKey(expression);
    }

//    public boolean contains(String group, Expression expression)
//    {
//        Expression rewritten = expression;
//
// TODO: broken... needs to rewrite children recursively
//        if (!expression.getArguments().isEmpty()) {
//            List<Expression> arguments = expression.getArguments().stream()
//                    .map(Reference.class::cast)
//                    .map(Reference::getName)
//                    .map(this::canonicalizeGroup)
//                    .map(Reference::new)
//                    .collect(Collectors.toList());
//            rewritten = expression.copyWithArguments(arguments);
//        }
//
//        return expressionsByGroup.get(group).containsKey(rewritten);
//    }

    private static class Node
    {
        public enum Type
        {
            GROUP, EXPRESSION
        }

        private final Type type;
        private final Object payload;
        private final boolean active;
        private final long version;

        public Node(Type type, Object payload, boolean active, long version)
        {
            this.active = active;
            this.type = type;
            this.payload = payload;
            this.version = version;
        }
    }

    private static class Edge
    {
        private final Type type;
        private final long version;
        private final String label;

        public enum Type
        {
            CONTAINS, REFERENCES, MERGED_WITH, REWRITTEN_TO, TRANSFORMED
        }

        public Edge(Type type, long version)
        {
            this(type, version, null);
        }

        public Edge(Type type, long version, String label)
        {
            this.type = type;
            this.version = version;
            this.label = label;
        }
    }

    public String toGraphviz()
    {
        Set<Integer> groupIds = new HashSet<>();

        Map<Object, Integer> ids = new HashMap<>();
        for (String group : groups.keySet()) {
            ids.put(group, ids.size());
            groupIds.add(ids.get(group));
        }
        for (Expression expression : expressions.keySet()) {
            ids.put(expression, ids.size());
        }

        Graph<Integer, String, Node, Edge, Void> graph = new Graph<>();
        DisjointSets<Integer> clusters = new DisjointSets<>();
        DisjointSets<Integer> ranks = new DisjointSets<>();

        for (Map.Entry<String, Long> entry : groups.entrySet()) {
            String group = entry.getKey();
            int id = ids.get(group);

            clusters.add(id);
            ranks.add(id);

            boolean active = !merges.containsKey(group);
            graph.addNode(id, new Node(Node.Type.GROUP, group, active, entry.getValue()));
        }

        for (Map.Entry<Expression, Long> entry : expressions.entrySet()) {
            Expression expression = entry.getKey();
            int id = ids.get(expression);

            clusters.add(id);
            ranks.add(id);

            boolean active = !rewrites.containsKey(expression);
            graph.addNode(id, new Node(Node.Type.EXPRESSION, expression, active, entry.getValue()));
        }

        // membership
        for (Map.Entry<String, Map<Expression, Long>> entry : expressionsByGroup.entrySet()) {
            String group = entry.getKey();
            int groupId = ids.get(group);
            for (Map.Entry<Expression, Long> versioned : entry.getValue().entrySet()) {
                int expressionId = ids.get(versioned.getKey());

                clusters.union(groupId, expressionId);
                graph.addEdge(groupId, ids.get(versioned.getKey()), new Edge(Edge.Type.CONTAINS, versioned.getValue()));
            }
        }

        // references
        for (Map.Entry<String, Map<Expression, Long>> entry : incomingReferences.entrySet()) {
            String group = entry.getKey();
            for (Map.Entry<Expression, Long> versioned : entry.getValue().entrySet()) {
                graph.addEdge(ids.get(versioned.getKey()), ids.get(group), new Edge(Edge.Type.REFERENCES, versioned.getValue()));
            }
        }

        // merges
        for (Map.Entry<String, VersionedItem<String>> entry : merges.entrySet()) {
            String source = entry.getKey();
            String target = entry.getValue().getItem();

            int sourceId = ids.get(source);
            int targetId = ids.get(target);

            clusters.union(sourceId, targetId);
            ranks.union(sourceId, targetId);

            graph.addEdge(sourceId, targetId, new Edge(Edge.Type.MERGED_WITH, entry.getValue().getVersion()));
        }

        // rewrites
        for (Map.Entry<Expression, VersionedItem<Expression>> entry : rewrites.entrySet()) {
            Expression from = entry.getKey();
            Expression to = entry.getValue().getItem();

            int fromId = ids.get(from);
            int toId = ids.get(to);

            clusters.union(fromId, toId);
            ranks.union(fromId, toId);

            graph.addEdge(fromId, toId, new Edge(Edge.Type.REWRITTEN_TO, entry.getValue().getVersion()));
        }

        // transformations
        for (Map.Entry<Expression, Map<Expression, VersionedItem<String>>> entry : transformations.entrySet()) {
            Expression from = entry.getKey();
            int fromId = ids.get(from);

            for (Map.Entry<Expression, VersionedItem<String>> edge : entry.getValue().entrySet()) {
                int toId;
                if (edge.getKey() instanceof Reference) {
                    toId = ids.get(((Reference) edge.getKey()).getName());
                }
                else {
                    toId = ids.get(edge.getKey());
                }
                graph.addEdge(fromId, toId, new Edge(Edge.Type.TRANSFORMED, edge.getValue().getVersion(), edge.getValue().getItem()));
            }
        }

        int i = 0;
        for (Set<Integer> nodes : clusters.sets()) {
            String clusterId = Integer.toString(i++);

            graph.addCluster(clusterId, null);
            for (int node : nodes) {
                graph.addNodeToCluster(node, clusterId);
            }
        }

        return graph.toGraphviz(
                () -> ImmutableMap.of("nodesep", "0.5"),
                (nodeId, node) -> {
                    Map<String, String> attributes = new HashMap<>();
                    attributes.put("label", node.payload.toString() + " @" + node.version);

                    if (node.type == Node.Type.GROUP) {
                        attributes.put("shape", "circle");
                    }
                    else {
                        attributes.put("shape", "rectangle");
                    }

                    if (!node.active) {
                        attributes.put("color", "grey");
                        attributes.put("fillcolor", "lightgrey");
                        attributes.put("style", "filled");
                    }

                    return attributes;
                },
                (from, to, edge) -> {
                    Map<String, String> attributes = new HashMap<>();

                    String label = "";
                    if (edge.label != null) {
                        label = edge.label + " @";
                    }
                    label += edge.version;
                    attributes.put("label", label);

                    if (!graph.getNode(from).get().active || !graph.getNode(to).get().active) {
                        attributes.put("color", "lightgrey");
                    }
                    switch (edge.type) {
                        case MERGED_WITH:
                        case REWRITTEN_TO:
                        case CONTAINS:
                            attributes.put("style", "dotted");
                            break;
                        case TRANSFORMED:
                            attributes.put("color", "blue");
                            attributes.put("penwidth", "2");
                            break;
                    }

                    return attributes;
                },
                (clusterId, cluster) -> {
                    List<String> result = new ArrayList<>();
                    result.add("style=dotted");

                    List<Integer> representatives = graph.getNodesInCluster(clusterId).stream()
                            .map(ranks::find)
                            .distinct()
                            .collect(Collectors.toList());

                    for (int node : representatives) {
                        StringBuilder value = new StringBuilder();
                        value.append("{ rank=");
                        if (groupIds.contains(node)) {
                            value.append("min");
                        }
                        else {
                            value.append("same");
                        }
                        value.append("; ");
                        value.append(Joiner.on(";").join(ranks.findAll(node)));
                        value.append(" }");

                        result.add(value.toString());
                    }
                    return result;
                });
    }
}
