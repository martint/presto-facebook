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
package com.facebook.presto.sql.optimizer.engine2;

import com.facebook.presto.sql.optimizer.tree2.Call;
import com.facebook.presto.sql.optimizer.tree2.Expression;
import com.facebook.presto.sql.optimizer.tree2.Lambda;
import com.facebook.presto.sql.optimizer.utils.DisjointSets;
import com.facebook.presto.sql.optimizer.utils.Graph;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.optimizer.engine2.GroupReference.group;
import static com.facebook.presto.sql.optimizer.tree2.Expressions.lambda;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class Memo
{
    private final boolean debug;

    private long version;
    private long nextGroupId;

    private final Set<Long> roots = new HashSet<>();
    private final Map<Long, Long> groupVersions = new HashMap<>();
    private final Map<Expression, Long> expressionVersions = new HashMap<>();

    private final Map<Long, Map<Expression, Long>> expressionsByGroup = new HashMap<>();
    private final Map<Expression, Long> expressionMembership = new HashMap<>();
    private final Map<Long, Map<Expression, Long>> incomingReferences = new HashMap<>();

    private final Map<Expression, VersionedItem<Expression>> rewrites = new HashMap<>();
    private final Map<Long, VersionedItem<Long>> merges = new HashMap<>();
    private final Map<Expression, Map<Expression, VersionedItem<String>>> transformations = new HashMap<>();

    public Memo()
    {
        this(false);
    }

    public Memo(boolean debug)
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
    public long insert(Expression expression)
    {
        long result = insertRecursive(expression);
        if (debug) {
            verify();
        }
        roots.add(result);
        return result;
    }

    /**
     * Records a transformation between "from" and "to".
     * <p>
     * Returns the rewritten "to", if any.
     */
    public Optional<Expression> transform(Expression from, Expression to, String reason)
    {
        checkArgument(expressionMembership.containsKey(from), "Unknown expression: %s when applying %s", from, reason);

        // Make sure we use the latest version of a group, otherwise, we
        // may end up attempting to merge groups that are already merged
        // TODO: do we really need to do this?
        long group = canonicalize(expressionMembership.get(from));

        if (to instanceof GroupReference) {
            // TODO: expression caused a change

            GroupReference reference = (GroupReference) to;
            if (reference.getId() != group) {
                mergeInto(group, reference.getId());
            }
            Expression target = new GroupReference(group);
            transformations.computeIfAbsent(canonicalize(from), e -> new HashMap<>())
                    .computeIfAbsent(target, e -> new VersionedItem<>(reason, version++));

            if (debug) {
                verify();
            }

            return Optional.ofNullable(to);
        }
        else {
            Expression rewritten = insertChildrenAndRewrite(to);

            // TODO: use contains()
            Long currentGroup = expressionMembership.get(rewritten);
            if (currentGroup == null) {
                // TODO: expression is new

                // If we've never seen this expression before, add it to
                // "from"'s group.
                insert(rewritten, group);
            }
            else if (!currentGroup.equals(group)) {
                // TODO: expression is not new, but it causes a change

                // If we've seen it before and its group is different from "from"'s group,
                // we've discovered an equivalence between two groups.
                mergeInto(group, currentGroup);
            }
            else {
                // TODO: expression is old and did not cause a change
            }

            transformations.computeIfAbsent(canonicalize(from), e -> new HashMap<>())
                    .computeIfAbsent(rewritten, e -> new VersionedItem<>(reason, version++));

            return Optional.of(rewritten);
        }
    }

    public long getGroup(Expression expression)
    {
        checkArgument(expressionMembership.containsKey(expression), "Unknown expression: %s", expression);
        return expressionMembership.get(expression);
    }

    public List<VersionedItem<Expression>> getExpressions(long group)
    {
        Set<Expression> canonical = expressionMembership.keySet().stream()
                .map(this::canonicalize)
                .collect(Collectors.toSet());

        return expressionsByGroup.get(group)
                .keySet().stream()
                .filter(e -> canonical.contains(e)) // pick only active expressions -- TODO: need a more efficient way to do this
                .map(e -> new VersionedItem<>(e, expressionVersions.get(e)))
                .collect(Collectors.toList());
    }

    private long insertRecursive(Expression expression)
    {
        checkArgument(!(expression instanceof GroupReference), "Expression cannot be a Group Reference: %s", expression);

        Expression rewritten = insertChildrenAndRewrite(expression);

        // TODO: use contains()?
        Long group = expressionMembership.get(rewritten);
        if (group == null) {
            group = createGroup();
            insert(rewritten, group);
        }

        return group;
    }

    /**
     * Inserts the children of the given expression and rewrites it in terms
     * of references to the corresponding groups.
     * <p>
     * It does *not* insert the top-level expression.
     */
    private Expression insertChildrenAndRewrite(Expression expression)
    {
        Expression result = expression;

        if (expression instanceof Call) {
            Call call = (Call) expression;

            List<Expression> arguments = call.getArguments().stream()
                    .map(argument -> {
                        if (argument instanceof GroupReference) {
                            // TODO: make sure group exists
                            return canonicalize(((GroupReference) argument).getId());
                        }

                        return insertRecursive(argument);
                    })
                    .map(GroupReference::new)
                    .collect(Collectors.toList());

            result = call.copyWithArguments(arguments);
        }
        else if (expression instanceof Lambda)
        {
            result = lambda(group(insertRecursive(((Lambda) expression).getBody())));
        }

        return result;
    }

    private void insert(Expression expression, long group)
    {
        checkArgument(!(expression instanceof GroupReference), "Cannot add a group reference %s to %s", expression, group);

        if (expression instanceof Call) {
            checkArgument(((Call) expression).getArguments().stream().allMatch(GroupReference.class::isInstance), "Expected all arguments to be group references: %s", expression);
        }

        expressionMembership.put(expression, group);
        expressionVersions.put(expression, version++);
        expressionsByGroup.get(group).put(expression, version++);

        if (expression instanceof Call) {
            ((Call) expression).getArguments().stream()
                    .map(GroupReference.class::cast)
                    .map(GroupReference::getId)
                    .forEach(child -> incomingReferences.get(child).putIfAbsent(expression, version++));
        }
        else if (expression instanceof Lambda) {
            Expression body = ((Lambda) expression).getBody();
            GroupReference reference = (GroupReference) body;

            incomingReferences.get(reference.getId()).putIfAbsent(expression, version++);
        }
    }

    private long createGroup()
    {
        long group = nextGroupId++;

        incomingReferences.put(group, new HashMap<>());
        expressionsByGroup.put(group, new HashMap<>());
        groupVersions.put(group, version++);

        return group;
    }

    private long canonicalize(long group)
    {
        while (merges.containsKey(group)) {
            group = merges.get(group).get();
        }
        return group;
    }

    private Expression canonicalize(Expression expression)
    {
        if (expression instanceof Call) {
            Call call = (Call) expression;
            checkArgument(call.getArguments().stream().allMatch(GroupReference.class::isInstance), "Expected all arguments to be references for: %s", expression);

            List<Expression> newArguments = call.getArguments().stream()
                    .map(GroupReference.class::cast)
                    .map(GroupReference::getId)
                    .map(this::canonicalize)
                    .map(GroupReference::new)
                    .collect(Collectors.toList());

            return call.copyWithArguments(newArguments);
        }
        else if (expression instanceof Lambda) {
            return lambda(canonicalize(((Lambda) expression).getBody()));
        }

        return expression;
    }

    private void mergeInto(long targetGroup, long group)
    {
        checkArgument(groupVersions.containsKey(targetGroup), "Group doesn't exist: %s", targetGroup);
        checkArgument(groupVersions.containsKey(group), "Group doesn't exist: %s", group);
        checkArgument(canonicalize(targetGroup) != canonicalize(group), "Groups are already merged: %s vs %s", targetGroup, group);

        merges.put(group, new VersionedItem<>(targetGroup, version++));

        // move all expressions to the target group
        for (Expression expression : expressionsByGroup.get(group).keySet()) {
            // TODO: canonicalize expression in case they have a recursive reference to the group that was just merged?
            expressionsByGroup.get(targetGroup).put(expression, version++);
            expressionMembership.put(expression, targetGroup);
        }

        // rewrite expressions that reference the merged group
        Map<Long, List<Expression>> referrerGroups = incomingReferences.get(group).keySet().stream()
                .collect(Collectors.groupingBy(expressionMembership::get));

        for (Map.Entry<Long, List<Expression>> entry : referrerGroups.entrySet()) {
            for (Expression referrerExpression : entry.getValue()) {
                long referrerGroup = entry.getKey();

                Expression expression = canonicalize(referrerExpression);

                // use contains()
                Long previousGroup = expressionMembership.get(expression);
                if (previousGroup == null) {
                    insert(expression, referrerGroup);
                }
                else if (!previousGroup.equals(referrerGroup)) {
                    mergeInto(referrerGroup, previousGroup);
                }

                if (!expression.equals(referrerExpression)) {
                    rewrites.put(referrerExpression, new VersionedItem<>(expression, version++));
                }
            }
        }
    }

    public void verify()
    {
        // ensure all active expressions "belong" to the canonical group name
        for (Map.Entry<Expression, Long> entry : expressionMembership.entrySet()) {
            Expression expression = entry.getKey();
            long group = entry.getValue();

            checkState(group == canonicalize(group),
                    "Expression not marked as belonging to canonical group: %s (%s vs %s)  ", expression, group, canonicalize(group));

            if (expression instanceof Call) {
                ((Call) expression).getArguments().stream()
                        .peek(e -> checkState((e instanceof GroupReference), "All expression arguments must be references: %s", expression))
                        .map(GroupReference.class::cast)
                        .peek(r -> checkState(r.getId() == canonicalize(r.getId()),
                                "Expression arguments must reference canonical groups: %s, %s vs %s", expression, r.getId(), canonicalize(r.getId())));
            }
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

        builder.append("== Roots ==\n");
        builder.append(Joiner.on(", ").join(roots.stream().map(r -> "$" + r).iterator()) + "\n");
        builder.append('\n');

        builder.append("== Groups ==\n");
        for (Map.Entry<Long, Map<Expression, Long>> entry : expressionsByGroup.entrySet()) {
            builder.append("$" + entry.getKey() + ": " + entry.getValue() + "\n");
        }
        builder.append('\n');

        builder.append("== Expressions ==\n");
        for (Map.Entry<Expression, Long> entry : expressionMembership.entrySet()) {
            builder.append(entry.getKey() + " ∈ $" + entry.getValue() + "\n");
        }
        builder.append('\n');

        builder.append("== References ==\n");
        for (Map.Entry<Long, Map<Expression, Long>> entry : incomingReferences.entrySet()) {
            for (Map.Entry<Expression, Long> versioned : entry.getValue().entrySet()) {
                builder.append(versioned.getKey() + " -> $" + entry.getKey() + " [" + versioned.getValue() + "]\n");
            }
        }
        builder.append('\n');

        builder.append("== Rewrites ==\n");
        for (Map.Entry<Expression, VersionedItem<Expression>> entry : rewrites.entrySet()) {
            builder.append(entry.getKey() + " -> " + entry.getValue().get() + " @" + entry.getValue().getVersion() + "\n");
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
        private final Object from;
        private final Object to;

        public enum Type
        {
            CONTAINS, REFERENCES, MERGED_WITH, REWRITTEN_TO, TRANSFORMED
        }

        public Edge(Type type, Object from, Object to, long version)
        {
            this(type, from, to, version, null);
        }

        public Edge(Type type, Object from, Object to, long version, String label)
        {
            this.type = type;
            this.from = from;
            this.to = to;
            this.version = version;
            this.label = label;
        }
    }

    public String toGraphviz()
    {
        return toGraphviz(e -> new HashMap<>(), (a, b) -> new HashMap<>());
    }

    public String toGraphviz(Function<Expression, Map<String, String>> nodeCustomizer, BiFunction<Object, Object, Map<String, String>> edgeCustomizer)
    {
        Set<Integer> groupIds = new HashSet<>();

        Map<Object, Integer> ids = new HashMap<>();
        for (Long group : groupVersions.keySet()) {
            ids.put(group, ids.size());
            groupIds.add(ids.get(group));
        }
        for (Expression expression : expressionVersions.keySet()) {
            ids.put(expression, ids.size());
        }

        Graph<Integer, String, Node, Edge, Void> graph = new Graph<>();
        DisjointSets<Integer> clusters = new DisjointSets<>();
        DisjointSets<Integer> ranks = new DisjointSets<>();

        for (Map.Entry<Long, Long> entry : groupVersions.entrySet()) {
            Long group = entry.getKey();
            int id = ids.get(group);

            clusters.add(id);
            ranks.add(id);

            boolean active = !merges.containsKey(group);
            graph.addNode(id, new Node(Node.Type.GROUP, group, active, entry.getValue()));
        }

        for (Map.Entry<Expression, Long> entry : expressionVersions.entrySet()) {
            Expression expression = entry.getKey();
            int id = ids.get(expression);

            clusters.add(id);
            ranks.add(id);

            boolean active = !rewrites.containsKey(expression);
            graph.addNode(id, new Node(Node.Type.EXPRESSION, expression, active, entry.getValue()));
        }

        // membership
        for (Map.Entry<Long, Map<Expression, Long>> entry : expressionsByGroup.entrySet()) {
            long group = entry.getKey();
            int groupId = ids.get(group);
            for (Map.Entry<Expression, Long> versioned : entry.getValue().entrySet()) {
                int expressionId = ids.get(versioned.getKey());

                clusters.union(groupId, expressionId);
                graph.addEdge(groupId, ids.get(versioned.getKey()), new Edge(Edge.Type.CONTAINS, group, versioned.getKey(), versioned.getValue()));
            }
        }

        // references
        for (Map.Entry<Long, Map<Expression, Long>> entry : incomingReferences.entrySet()) {
            long group = entry.getKey();
            for (Map.Entry<Expression, Long> versioned : entry.getValue().entrySet()) {
                graph.addEdge(ids.get(versioned.getKey()), ids.get(group), new Edge(Edge.Type.REFERENCES, versioned.getKey(), group, versioned.getValue()));
            }
        }

        // merges
        for (Map.Entry<Long, VersionedItem<Long>> entry : merges.entrySet()) {
            long source = entry.getKey();
            long target = entry.getValue().get();

            int sourceId = ids.get(source);
            int targetId = ids.get(target);

            clusters.union(sourceId, targetId);
            ranks.union(sourceId, targetId);

            graph.addEdge(sourceId, targetId, new Edge(Edge.Type.MERGED_WITH, source, target, entry.getValue().getVersion()));
        }

        // rewrites
        for (Map.Entry<Expression, VersionedItem<Expression>> entry : rewrites.entrySet()) {
            Expression from = entry.getKey();
            Expression to = entry.getValue().get();

            int fromId = ids.get(from);
            int toId = ids.get(to);

            clusters.union(fromId, toId);
            ranks.union(fromId, toId);

            graph.addEdge(fromId, toId, new Edge(Edge.Type.REWRITTEN_TO, from, to, entry.getValue().getVersion()));
        }

        // transformations
        for (Map.Entry<Expression, Map<Expression, VersionedItem<String>>> entry : transformations.entrySet()) {
            Expression from = entry.getKey();
            int fromId = ids.get(from);

            for (Map.Entry<Expression, VersionedItem<String>> edge : entry.getValue().entrySet()) {
                int toId;
                if (edge.getKey() instanceof GroupReference) {
                    toId = ids.get(((GroupReference) edge.getKey()).getId());
                }
                else {
                    toId = ids.get(edge.getKey());
                }
                graph.addEdge(fromId, toId, new Edge(Edge.Type.TRANSFORMED, from, edge.getKey(), edge.getValue().getVersion(), edge.getValue().get()));
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

                    if (roots.contains(node.payload)) {
                        attributes.put("penwidth", "3");
                    }

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

                    if (node.type == Node.Type.EXPRESSION) {
                        attributes.putAll(nodeCustomizer.apply((Expression) node.payload));
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
                        case CONTAINS:
                            attributes.put("arrowhead", "dot");
                            break;
                        case MERGED_WITH:
                        case REWRITTEN_TO:
                            attributes.put("style", "dotted");
                            break;
                        case TRANSFORMED:
                            attributes.put("color", "blue");
                            attributes.put("penwidth", "2");
                            break;
                    }

                    if (edge.type == Edge.Type.CONTAINS || edge.type == Edge.Type.REFERENCES) {
                        attributes.putAll(edgeCustomizer.apply(edge.from, edge.to));
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

//                    if (roots.stream().map(ids::get).anyMatch(graph.getNodesInCluster(clusterId)::contains)) {
//                        result.add("penwidth=2");
//                    }
//                    else {
//                    }

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
