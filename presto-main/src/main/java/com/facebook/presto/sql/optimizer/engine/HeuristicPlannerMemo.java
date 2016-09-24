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
import com.facebook.presto.sql.optimizer.utils.Graph;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.optimizer.engine.GroupReference.group;
import static com.facebook.presto.sql.optimizer.engine.Utils.getChildren;
import static com.facebook.presto.sql.optimizer.tree.Expressions.lambda;

public class HeuristicPlannerMemo
{
    private final long root;

    private long nextGroupId;
    private final Map<Long, Expression> mainExpressions = new HashMap<>();
    private final Map<Long, Set<Expression>> oldExpressions = new HashMap<>();
    private final Map<Expression, Expression> transformations = new HashMap<>();

    public HeuristicPlannerMemo(Expression expression)
    {
        root = insert(expression);
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
    public void transform(long group, Expression to, String reason)
    {
        Expression old = mainExpressions.get(group);
        oldExpressions.computeIfAbsent(group, (key) -> new HashSet<>())
                .add(old);

        Expression expression = insertChildrenAndRewrite(to);
        mainExpressions.put(group, expression);
        transformations.put(old, expression);
    }

    private long insert(Expression expression)
    {
        if (expression instanceof GroupReference) {
            return ((GroupReference) expression).getId();
        }

        Expression rewritten = insertChildrenAndRewrite(expression);
        long group = createGroup();
        mainExpressions.put(group, rewritten);

        return group;
    }

    private Expression insertChildrenAndRewrite(Expression expression)
    {
        Function<Expression, Expression> processor = argument -> {
            if (argument instanceof GroupReference || argument instanceof Atom) {
                return argument;
            }

            return group(insert(argument));
        };

        Expression result = expression;

        if (expression instanceof Apply) {
            Apply apply = (Apply) expression;

            List<Expression> arguments = apply.getArguments().stream()
                    .map(processor)
                    .collect(Collectors.toList());

            Expression target = apply.getTarget();
            if (!(target instanceof Reference)) {
                target = processor.apply(target);
            }
            result = new Apply(target, arguments);
        }
        else if (expression instanceof Lambda) {
            Lambda lambda = (Lambda) expression;
            if (lambda.getBody() instanceof GroupReference) {
                return lambda;
            }

            result = lambda(group(insert(((Lambda) expression).getBody())));
        }

        return result;
    }

    private long createGroup()
    {
        return nextGroupId++;
    }

    public String dump()
    {
        StringBuilder builder = new StringBuilder();

//        builder.append("== Roots ==\n");
//        builder.append(Joiner.on(", ").join(roots.stream().map(r -> "$" + r).iterator()) + "\n");
//        builder.append('\n');
//
//        builder.append("== Groups ==\n");
//        for (Map.Entry<Long, Map<Expression, Long>> entry : expressionsByGroup.entrySet()) {
//            builder.append("$" + entry.getKey() + ": " + entry.getValue() + "\n");
//        }
//        builder.append('\n');
//
//        builder.append("== Expressions ==\n");
//        for (Map.Entry<Expression, Long> entry : expressionMembership.entrySet()) {
//            builder.append(entry.getKey() + " ∈ $" + entry.getValue() + "\n");
//        }
//        builder.append('\n');
//
//        builder.append("== References ==\n");
//        for (Map.Entry<Long, Map<Expression, Long>> entry : incomingReferences.entrySet()) {
//            for (Map.Entry<Expression, Long> versioned : entry.getValue().entrySet()) {
//                builder.append(versioned.getKey() + " -> $" + entry.getKey() + " [" + versioned.getValue() + "]\n");
//            }
//        }
//        builder.append('\n');
//
// TODO
//        builder.append("== Rewrites ==\n");
//        for (Map.Entry<Expression, VersionedItem<Expression>> entry : rewrites.entrySet()) {
//            builder.append(entry.getKey() + " -> " + entry.getValue().get() + " @" + entry.getValue().getVersion() + "\n");
//        }
//
        return builder.toString();
    }

    public Expression getExpression(long group)
    {
        return mainExpressions.get(group);
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
        // assign global unique id across groups and expressions
        Map<Object, Integer> ids = new HashMap<>();
        for (Long group : mainExpressions.keySet()) {
            ids.put(group, ids.size());
        }

        Set<Expression> allExpressions = new HashSet<>();
        allExpressions.addAll(mainExpressions.values());
        oldExpressions.values().stream().forEach(allExpressions::addAll);

        for (Expression expression : allExpressions) {
            ids.put(expression, ids.size());
        }

        Graph<Integer, String, Node, Edge, Void> graph = new Graph<>();

        // groups
        for (long group : mainExpressions.keySet()) {
            int id = ids.get(group);
            graph.addNode(id, new Node(Node.Type.GROUP, group, true, 0));
        }

        // expresssions
        for (Expression expression : mainExpressions.values()) {
            int id = ids.get(expression);
            graph.addNode(id, new Node(Node.Type.EXPRESSION, expression, true, 0));
        }

        for (Set<Expression> old : oldExpressions.values()) {
            for (Expression expression : old) {
                int id = ids.get(expression);
                graph.addNode(id, new Node(Node.Type.EXPRESSION, expression, false, 0));
            }
        }

        // references
        for (Expression expression : mainExpressions.values()) {
            int from = ids.get(expression);
            for (Expression child : getChildren(expression)) {
                if (child instanceof GroupReference) {
                    int to = ids.get(((GroupReference) child).getId());
                    graph.addEdge(from, to, new Edge(Edge.Type.REFERENCES, from, to, 0));
                }
            }
        }

        for (Set<Expression> expressions : oldExpressions.values()) {
            for (Expression expression : expressions) {
                int from = ids.get(expression);
                for (Expression child : getChildren(expression)) {
                    if (child instanceof GroupReference) {
                        int to = ids.get(((GroupReference) child).getId());
                        graph.addEdge(from, to, new Edge(Edge.Type.REFERENCES, from, to, 0));
                    }
                }
            }
        }

        // transformations
        for (Map.Entry<Expression, Expression> entry : transformations.entrySet()) {
            int from = ids.get(entry.getKey());
            int to = ids.get(entry.getValue());

            graph.addEdge(from, to, new Edge(Edge.Type.TRANSFORMED, from, to, 0));
        }

        return graph.toGraphviz(
                () -> ImmutableMap.of("nodesep", "0.5"),
                (nodeId, node) -> {
                    Map<String, String> attributes = new HashMap<>();
                    attributes.put("label", node.payload.toString());

                    if (node.payload.equals(nodeId)) {
                        attributes.put("penwidth", "3");
                    }

                    attributes.put("shape", "rectangle");
                    attributes.putAll(nodeCustomizer.apply((Expression) node.payload));

                    return attributes;
                },
                (from, to, edge) -> {
                    Map<String, String> attributes = new HashMap<>();

                    String label = "";
                    if (edge.label != null) {
                        label = edge.label + " v";
                    }
                    attributes.put("label", label);
                    attributes.putAll(edgeCustomizer.apply(edge.from, edge.to));
                    return attributes;
                },
                (clusterId, cluster) -> new ArrayList<>());

//        Set<Integer> groupIds = new HashSet<>();
//
//        Map<Object, Integer> ids = new HashMap<>();
//        for (Long group : groupVersions.keySet()) {
//            ids.put(group, ids.size());
//            groupIds.add(ids.get(group));
//        }
//        for (Expression expression : expressionVersions.keySet()) {
//            ids.put(expression, ids.size());
//        }
//
//        Graph<Integer, String, Node, Edge, Void> graph = new Graph<>();
//        DisjointSets<Integer> clusters = new DisjointSets<>();
//        DisjointSets<Integer> ranks = new DisjointSets<>();
//
//        for (Map.Entry<Long, Long> entry : groupVersions.entrySet()) {
//            Long group = entry.getKey();
//            int id = ids.get(group);
//
//            clusters.add(id);
//
//            boolean active = true; //!merges.containsKey(group);
//
////            if (active) {
//            ranks.add(id);
//            graph.addNode(id, new Node(Node.Type.GROUP, group, active, entry.getValue()));
////            }
//        }
//
//        for (Map.Entry<Expression, Long> entry : expressionVersions.entrySet()) {
//            Expression expression = entry.getKey();
//            int id = ids.get(expression);
//
//            clusters.add(id);
//
//            boolean active = true; //!rewrites.containsKey(expression);
//
////            if (active) {
//            ranks.add(id);
//            graph.addNode(id, new Node(Node.Type.EXPRESSION, expression, active, entry.getValue()));
////            }
//        }
//
//        // membership
//        for (Map.Entry<Long, Map<Expression, Long>> entry : expressionsByGroup.entrySet()) {
//            long group = entry.getKey();
//            int groupId = ids.get(group);
//            for (Map.Entry<Expression, Long> versioned : entry.getValue().entrySet()) {
//                int expressionId = ids.get(versioned.getKey());
//
//                try {
//                    graph.addEdge(groupId, ids.get(versioned.getKey()), new Edge(Edge.Type.CONTAINS, group, versioned.getKey(), versioned.getValue()));
//                    clusters.union(groupId, expressionId);
//                }
//                catch (Exception e) {
//                }
//            }
//        }
//
//        // references
//        for (Map.Entry<Long, Map<Expression, Long>> entry : incomingReferences.entrySet()) {
//            long group = entry.getKey();
//            for (Map.Entry<Expression, Long> versioned : entry.getValue().entrySet()) {
//                try {
//                    graph.addEdge(ids.get(versioned.getKey()), ids.get(group), new Edge(Edge.Type.REFERENCES, versioned.getKey(), group, versioned.getValue()));
//                }
//                catch (Exception e) {
//                }
//            }
//        }
//
//        // merges
//        for (long group : merges.roots()) {
//            for (long source : merges.findAll(group)) {
//                if (group == source) {
//                    continue;
//                }
//
//                int sourceId = ids.get(source);
//                int targetId = ids.get(group);
//                graph.addEdge(sourceId, targetId, new Edge(Edge.Type.MERGED_WITH, source, group, 0));
//                clusters.union(sourceId, targetId);
//                ranks.union(sourceId, targetId);
//            }
//        }
////        for (Map.Entry<Long, VersionedItem<Long>> entry : merges.entrySet()) {
////            long source = entry.getKey();
////            long target = entry.getValue().get();
////
////            int sourceId = ids.get(source);
////            int targetId = ids.get(target);
////
////            try {
////                graph.addEdge(sourceId, targetId, new Edge(Edge.Type.MERGED_WITH, source, target, entry.getValue().getVersion()));
////                clusters.union(sourceId, targetId);
////                ranks.union(sourceId, targetId);
////            }
////            catch (Exception e) {
////            }
////        }
//
//        // rewrites
//        // TODO
////        for (Map.Entry<Expression, VersionedItem<Expression>> entry : rewrites.entrySet()) {
////            Expression from = entry.getKey();
////            Expression to = entry.getValue().get();
////
////            int fromId = ids.get(from);
////            int toId = ids.get(to);
////
////            try {
////                graph.addEdge(fromId, toId, new Edge(Edge.Type.REWRITTEN_TO, from, to, entry.getValue().getVersion()));
////                clusters.union(fromId, toId);
////                ranks.union(fromId, toId);
////            }
////            catch (Exception e) {
////            }
////        }
//
//        // transformations
//        for (Map.Entry<Expression, Map<Expression, VersionedItem<String>>> entry : transformations.entrySet()) {
//            Expression from = entry.getKey();
//            int fromId = ids.get(from);
//
//            for (Map.Entry<Expression, VersionedItem<String>> edge : entry.getValue().entrySet()) {
//                int toId;
//                if (edge.getKey() instanceof GroupReference) {
//                    toId = ids.get(((GroupReference) edge.getKey()).getId());
//                }
//                else {
//                    toId = ids.get(edge.getKey());
//                }
//
//                try {
//                    graph.addEdge(fromId, toId, new Edge(Edge.Type.TRANSFORMED, from, edge.getKey(), edge.getValue().getVersion(), edge.getValue().get()));
//                }
//                catch (Exception e) {
//                }
//            }
//        }
//
//        int i = 0;
//        for (Set<Integer> nodes : clusters.sets()) {
//            String clusterId = Integer.toString(i++);
//
//            graph.addCluster(clusterId, null);
//            for (int node : nodes) {
//                try {
//                    graph.addNodeToCluster(node, clusterId);
//                }
//                catch (Exception e) {
//                }
//            }
//        }
//
//        return graph.toGraphviz(
//                () -> ImmutableMap.of("nodesep", "0.5"),
//                (nodeId, node) -> {
//                    Map<String, String> attributes = new HashMap<>();
//                    attributes.put("label", node.payload.toString() + " v" + node.version);
//
//                    if (roots.contains(node.payload)) {
//                        attributes.put("penwidth", "3");
//                    }
//
//                    if (node.type == Node.Type.GROUP) {
//                        attributes.put("shape", "circle");
//                        attributes.put("label", "$" + node.payload.toString() + " v" + node.version);
//                    }
//                    else {
//                        attributes.put("shape", "rectangle");
//                    }
//
//                    if (!node.active) {
//                        attributes.put("color", "grey");
//                        attributes.put("fillcolor", "lightgrey");
//                        attributes.put("style", "filled");
//                    }
//
//                    if (node.type == Node.Type.EXPRESSION) {
//                        attributes.putAll(nodeCustomizer.apply((Expression) node.payload));
//                    }
//
//                    return attributes;
//                },
//                (from, to, edge) -> {
//                    Map<String, String> attributes = new HashMap<>();
//
//                    String label = "";
//                    if (edge.label != null) {
//                        label = edge.label + " v";
//                    }
//                    label += edge.version;
//                    attributes.put("label", label);
//
//                    if (!graph.getNode(from).get().active || !graph.getNode(to).get().active) {
//                        attributes.put("color", "lightgrey");
//                    }
//                    switch (edge.type) {
//                        case CONTAINS:
//                            attributes.put("arrowhead", "dot");
//                            break;
//                        case MERGED_WITH:
//                        case REWRITTEN_TO:
//                            attributes.put("style", "dotted");
//                            break;
//                        case TRANSFORMED:
//                            attributes.put("color", "blue");
//                            attributes.put("penwidth", "2");
//                            break;
//                    }
//
//                    if (edge.type == Edge.Type.CONTAINS || edge.type == Edge.Type.REFERENCES) {
//                        attributes.putAll(edgeCustomizer.apply(edge.from, edge.to));
//                    }
//
//                    return attributes;
//                },
//                (clusterId, cluster) -> {
//                    List<String> result = new ArrayList<>();
//                    result.add("style=dotted");
//
//                    List<Integer> representatives = graph.getNodesInCluster(clusterId).stream()
//                            .map(ranks::find)
//                            .distinct()
//                            .collect(Collectors.toList());
//
////                    if (roots.stream().map(ids::get).anyMatch(graph.getNodesInCluster(clusterId)::contains)) {
////                        result.add("penwidth=2");
////                    }
////                    else {
////                    }
//
//                    for (int node : representatives) {
//                        StringBuilder value = new StringBuilder();
//                        value.append("{ rank=");
//                        if (groupIds.contains(node)) {
//                            value.append("min");
//                        }
//                        else {
//                            value.append("same");
//                        }
//                        value.append("; ");
//                        value.append(Joiner.on(";").join(ranks.findAll(node)));
//                        value.append(" }");
//
//                        result.add(value.toString());
//                    }
//                    return result;
//                });
    }

    //    }
//        return expressionsByGroup.get(group).containsKey(rewritten);
//
//        }
//            rewritten = expression.copyWithArguments(arguments);
//                    .collect(Collectors.toList());
//                    .map(Reference::new)
//                    .map(this::canonicalizeGroup)
//                    .map(Reference::getName)
//                    .map(Reference.class::cast)
//            List<Expression> arguments = expression.getArguments().stream()
//        if (!expression.getArguments().isEmpty()) {
// TODO: broken... needs to rewrite children recursively
//
//        Expression rewritten = expression;
//    {
//    }
//        }
//            }
//                checkState(expressionMembership.containsKey(expression), "Expression in incomingReferences for group %s but not in expressionMembership: %s", group, expression);
//            for (Expression expression : entry.getValue()) {
//
//            checkState(expressionsByGroup.containsKey(group), "Group exists in incomingReferences but not in expressionsByGroup: %s", group);
//            String group = entry.getKey();
//        for (Map.Entry<String, Set<Expression>> entry : incomingReferences.entrySet()) {
//
//        }
//            checkState(expressionsByGroup.get(group).contains(expression), "expressionsByGroup does not contain expression declared by expressionMembership: %s, %s", group, expression);
//            checkState(expressionsByGroup.containsKey(group), "Group in expressionMembership but not in expressionsByGroup: %s", group);
//
//            String group = entry.getValue();
//            Expression expression = entry.getKey();
//        for (Map.Entry<Expression, String> entry : expressionMembership.entrySet()) {
//
//        }
//            }
//                        group);
//                        expressionMembership.get(expression),
//                        expression,
//                checkState(expressionMembership.get(expression).equals(group), "Membership for expression doesn't match group that contains it: %s, %s vs %s",
//            for (VersionedItem<Expression> expression : entry.getValue()) {
//
//            checkState(incomingReferences.containsKey(group), "Group in expressionsByGroup but not in incomingReferences: %s", group);
//
//            String group = entry.getKey();
//        for (Map.Entry<String, Set<VersionedItem<Expression>>> entry : expressionsByGroup.entrySet()) {
//    {
//    }
//        return expression;
//        }
//            expression = rewrites.get(expression).get();
//        while (rewrites.containsKey(expression)) {
//    {
//    }
//        return group;
//        }
//            group = merges.get(group).get();
//        while (merges.containsKey(group)) {
//    {
    private static class ExpressionAndGroup
    {
        private final long group;
        private final Expression expression;

        public ExpressionAndGroup(long group, Expression expression)
        {
            this.group = group;
            this.expression = expression;
        }

        public long getGroup()
        {
            return group;
        }

        public Expression getExpression()
        {
            return expression;
        }
    }
}
