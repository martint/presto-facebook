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

import com.facebook.presto.sql.optimizer.tree.Apply;
import com.facebook.presto.sql.optimizer.tree.Atom;
import com.facebook.presto.sql.optimizer.tree.Expression;
import com.facebook.presto.sql.optimizer.tree.Lambda;
import com.facebook.presto.sql.optimizer.tree.Reference;
import com.facebook.presto.sql.optimizer.utils.DisjointSets;
import com.facebook.presto.sql.optimizer.utils.Graph;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.optimizer.engine.GroupReference.group;
import static com.facebook.presto.sql.optimizer.tree.Expressions.lambda;
import static com.google.common.base.Preconditions.checkArgument;

public class HeuristicPlannerMemo
{
    private long nextGroupId;

    private final long root;

    private final Map<Long, Expression> currentExpressionByGroup = new HashMap<>();
    private final Map<Long, Set<Expression>> expressionsByGroup = new HashMap<>();
    private final Map<Expression, Long> expressionMembership = new HashMap<>();
    private final Map<Long, Set<Expression>> incomingReferences = new HashMap<>();

    private final Map<Expression, Map<Expression, String>> transformations = new HashMap<>();

    private final Map<Long, Long> equivalences = new HashMap<>();

    public HeuristicPlannerMemo(Expression expression)
    {
        root = insertRecursive(expression);
    }

    public long getRoot()
    {
        return root;
    }

    /**
     * Records a transformation between "from" and "to".
     * <p>
     * Returns the rewritten "to", if any.
     */
    public boolean transform(Expression from, Expression to, String reason)
    {
        checkArgument(expressionMembership.containsKey(from), "Unknown expression: %s when applying %s", from, reason);

        long sourceGroup = expressionMembership.get(from);

        Expression rewritten;
        Long targetGroup;
        if (to instanceof GroupReference) {
            targetGroup = ((GroupReference) to).getId();
            rewritten = to;
        }
        else {
            rewritten = insertChildrenAndRewrite(to);
            targetGroup = expressionMembership.get(rewritten);
        }

        if (targetGroup == null) {
//            targetGroup = createGroup();
            insert(rewritten, sourceGroup);
            currentExpressionByGroup.put(sourceGroup, rewritten);
        }
        else if (!targetGroup.equals(sourceGroup)) {
            equivalences.put(sourceGroup, targetGroup);
        }
        else {
            throw new RuntimeException("shouldn't happen?");
//            currentExpressionByGroup.put(targetGroup, rewritten);
        }

        String existing = transformations.computeIfAbsent(from, e -> new HashMap<>())
                .putIfAbsent(rewritten, reason);

        return existing == null;
    }

    public Expression getExpression(long group)
    {
        long targetGroup = find(group);
        return currentExpressionByGroup.get(targetGroup);
    }

    private long insertRecursive(Expression expression)
    {
        checkArgument(!(expression instanceof GroupReference), "Expression cannot be a Group Reference: %s", expression);

        Expression rewritten = insertChildrenAndRewrite(expression);

        Long group = expressionMembership.get(rewritten);
        if (group == null) {
            group = createGroup();
            insert(rewritten, group);
            currentExpressionByGroup.put(group, rewritten);
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

        if (expression instanceof Apply) {
            Apply apply = (Apply) expression;

            Function<Expression, Expression> processor = argument -> {
                if (argument instanceof Atom) {
                    return argument;
                }

                if (argument instanceof GroupReference) {
                    // TODO: make sure group exists
                    return argument;
                }
                return group(argument.type(), insertRecursive(argument));
            };

            List<Expression> arguments = apply.getArguments().stream()
                    .map(processor)
                    .collect(Collectors.toList());

            Expression target = apply.getTarget();
            if (!(target instanceof Reference)) {
                target = processor.apply(target);
            }
            result = new Apply(expression.type(), target, arguments);
        }
        else if (expression instanceof Lambda) {
            Lambda lambda = (Lambda) expression;
            if (lambda.getBody() instanceof GroupReference) {
                return lambda;
            }

            Expression body = ((Lambda) expression).getBody();
            result = lambda(lambda.type(), group(body.type(), insertRecursive(body)));
        }

        return result;
    }

    private void insert(Expression expression, long group)
    {
        if (expression instanceof Apply) {
            checkArgument(((Apply) expression).getArguments().stream().allMatch(e -> e instanceof GroupReference || e instanceof Atom), "Expected all arguments to be group references or atoms: %s", expression);
        }

        expressionMembership.put(expression, group);
//        expressionVersions.put(expression, version++);
        expressionsByGroup.get(group).add(expression);

        if (expression instanceof Apply) {
            Apply apply = (Apply) expression;
            if (apply.getTarget() instanceof GroupReference) {
                incomingReferences.get(((GroupReference) apply.getTarget()).getId()).add(expression);
            }

            apply.getArguments().stream()
                    .filter(GroupReference.class::isInstance)
                    .map(GroupReference.class::cast)
                    .map(GroupReference::getId)
                    .forEach(child -> incomingReferences.get(child).add(expression));
        }
        else if (expression instanceof Lambda) {
            Expression body = ((Lambda) expression).getBody();
            GroupReference reference = (GroupReference) body;

            incomingReferences.get(reference.getId()).add(expression);
        }
    }

    private long createGroup()
    {
        long group = nextGroupId++;

        incomingReferences.put(group, new HashSet<>());
        expressionsByGroup.put(group, new HashSet<>());
//        groupVersions.put(group, version++);
//        merges.add(group);

        return group;
    }

    public boolean isValid(long group)
    {
        return true; // find(group) == group;
    }

    private long find(long group)
    {
        while (equivalences.containsKey(group)) {
            group = equivalences.get(group);
        }
        return group;
    }

//    private Expression find(Expression expression)
//    {
//        while (rewrites.containsKey(expression)) {
//            expression = rewrites.get(expression).get();
//        }
//        return expression;
//    }

//    public Expression canonicalize(Expression expression)
//    {
//        if (expression instanceof Apply) {
//            Apply apply = (Apply) expression;
//
//            List<Expression> newArguments = apply.getArguments().stream()
//                    .map(this::canonicalize)
//                    .collect(Collectors.toList());
//
//            return new Apply(expression.type(), canonicalize(apply.getTarget()), newArguments);
//        }
//        else if (expression instanceof Lambda) {
//            return lambda(canonicalize(((Lambda) expression).getBody()));
//        }
//        else if (expression instanceof GroupReference) {
//            return new GroupReference((((GroupReference) expression).getId()));
//        }
//
//        return expression;
//    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();

        Queue<Long> queue = new ArrayDeque<>();
        queue.add(root);

        Set<Long> visited = new HashSet<>();
        while (!queue.isEmpty()) {
            long group = queue.poll();

            if (visited.contains(group)) {
                continue;
            }

            Expression expression = getExpression(group);
            if (expression instanceof Apply) {
                Apply apply = (Apply) expression;

                Expression target = apply.getTarget();
                if (target instanceof GroupReference) {
                    queue.add(((GroupReference) target).getId());
                }

                for (Expression argument : apply.getArguments()) {
                    if (argument instanceof GroupReference) {
                        queue.add(((GroupReference) argument).getId());
                    }
                }
            }
            else if (expression instanceof Lambda) {
                Lambda lambda = (Lambda) expression;

                if (lambda.getBody() instanceof GroupReference) {
                    queue.add(((GroupReference) lambda.getBody()).getId());
                }
            }

            builder.append("$" + group + " := " + expression + " :: " + expression.type() + "\n");
        }

        return builder.toString();
    }

    private static class PendingMerge
    {
        private final long source;
        private final long target;

        public PendingMerge(long source, long target)
        {
            this.source = source;
            this.target = target;
        }

        public long getSource()
        {
            return source;
        }

        public long getTarget()
        {
            return target;
        }
    }

    public boolean contains(Expression expression)
    {
        return expressionMembership.containsKey(expression);
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
        for (Long group : expressionsByGroup.keySet()) {
            ids.put(group, ids.size());
            groupIds.add(ids.get(group));
        }
        for (Expression expression : expressionMembership.keySet()) {
            ids.put(expression, ids.size());
        }

        Graph<Integer, String, Node, Edge, Void> graph = new Graph<>();
        DisjointSets<Integer> clusters = new DisjointSets<>();
        DisjointSets<Integer> ranks = new DisjointSets<>();

        for (Long group : expressionsByGroup.keySet()) {
            int id = ids.get(group);

            clusters.add(id);

            ranks.add(id);
            graph.addNode(id, new Node(Node.Type.GROUP, group, true, 0));
        }

        for (Expression expression : expressionMembership.keySet()) {
            int id = ids.get(expression);

            clusters.add(id);

            ranks.add(id);
            graph.addNode(id, new Node(Node.Type.EXPRESSION, expression, true, 0));
        }

        // membership
        for (Map.Entry<Long, Set<Expression>> entry : expressionsByGroup.entrySet()) {
            long group = entry.getKey();
            int groupId = ids.get(group);
            for (Expression expression : entry.getValue()) {
                int expressionId = ids.get(expression);

                try {
                    graph.addEdge(groupId, ids.get(expression), new Edge(Edge.Type.CONTAINS, group, expression, 0));
                    clusters.union(groupId, expressionId);
                }
                catch (Exception e) {
                }
            }
        }

        // references
        for (Map.Entry<Long, Set<Expression>> entry : incomingReferences.entrySet()) {
            long group = entry.getKey();
            for (Expression expression : entry.getValue()) {
                try {
                    graph.addEdge(ids.get(expression), ids.get(group), new Edge(Edge.Type.REFERENCES, expression, group, 0));
                }
                catch (Exception e) {
                }
            }
        }

        // group equivalences
        for (Map.Entry<Long, Long> entry : equivalences.entrySet()) {
            int sourceId = ids.get(entry.getKey());
            int targetId = ids.get(entry.getValue());

//                clusters.union(sourceId, targetId);

            graph.addEdge(sourceId, targetId, new Edge(Edge.Type.MERGED_WITH, entry.getKey(), entry.getValue(), 0));
        }

        // transformations
        for (Map.Entry<Expression, Map<Expression, String>> entry : transformations.entrySet()) {
            Expression from = entry.getKey();
            int fromId = ids.get(from);

            for (Map.Entry<Expression, String> edge : entry.getValue().entrySet()) {
                int toId;
                if (edge.getKey() instanceof GroupReference) {
                    toId = ids.get(((GroupReference) edge.getKey()).getId());
                }
                else {
                    toId = ids.get(edge.getKey());
                }

                try {
                    graph.addEdge(fromId, toId, new Edge(Edge.Type.TRANSFORMED, from, edge.getKey(), 0, edge.getValue()));
                }
                catch (Exception e) {
                }
            }
        }

        int i = 0;
        for (Set<Integer> nodes : clusters.sets()) {
            String clusterId = Integer.toString(i++);

            graph.addCluster(clusterId, null);
            for (int node : nodes) {
                try {
                    graph.addNodeToCluster(node, clusterId);
                }
                catch (Exception e) {
                }
            }
        }

        return graph.toGraphviz(
                () -> ImmutableMap.of("nodesep", "0.5"),
                (nodeId, node) -> {
                    Map<String, String> attributes = new HashMap<>();
                    attributes.put("label", node.payload.toString() + " v" + node.version);

                    if (node.payload.equals(root)) {
                        attributes.put("penwidth", "3");
                    }

                    if (node.type == Node.Type.GROUP) {
                        attributes.put("shape", "circle");
                        attributes.put("label", "$" + node.payload.toString() + " v" + node.version);
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
                        label = edge.label + " v";
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

                    if (graph.getNodesInCluster(clusterId).contains(root)) {
                        result.add("penwidth=2");
                    }
                    else {
                    }

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
