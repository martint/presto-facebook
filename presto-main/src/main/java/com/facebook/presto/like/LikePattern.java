package com.facebook.presto.like;

import com.facebook.presto.graph.Edge;
import com.facebook.presto.graph.Graph;
import com.facebook.presto.graph.GraphvizPrinter;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static com.google.common.base.Functions.forMap;
import static com.google.common.base.Predicates.equalTo;

public class LikePattern
{
    public static void compile(String pattern, char escapeChar)
    {
        DfaBuilder builder = new DfaBuilder();

        boolean escaped = false;
        for (int i = 0; i < pattern.length(); i++) {
            char currentChar = pattern.charAt(i);

            if (currentChar == escapeChar) {
                escaped = true;
            }
            else if (escaped) {
                builder.matchCharacter(currentChar);
            }
            else {
                switch (currentChar) {
                    case '%':
                        builder.matchZeroOrMore();
                        break;
                    case '_':
                        builder.matchAny();
                        break;
                    default:
                        builder.matchCharacter(currentChar);
                }
                escaped = false;
            }
        }

        builder.build();


    }

    private static class DfaBuilder
    {
        private int nextPosition;

        private final Map<Integer, Optional<Character>> symbolForPosition = new HashMap<>();
        private final SetMultimap<Integer, Integer> followPositions = HashMultimap.create();

        private final Set<Integer> firstPositions = new HashSet<>();

        private boolean foundFirstNonNullablePosition = false;
        private final Set<Integer> lastPositions = new HashSet<>();

        private final Set<Character> alphabet = new HashSet<>();

        public void matchCharacter(char character)
        {
            matchOne(Optional.of(character));
        }

        public void matchAny()
        {
            matchOne(Optional.<Character>absent());
        }

        public void matchZeroOrMore()
        {
            Optional<Character> symbol = Optional.absent();

            int position = nextPosition++;
            symbolForPosition.put(position, symbol);

            // loopback
            followPositions.put(position, position);

            // link last positions to current position
            for (Integer lastPosition : lastPositions) {
                followPositions.put(lastPosition, position);
            }

            if (!foundFirstNonNullablePosition) {
                firstPositions.add(position);
            }

            // because this can match 0 times, add to the list of last positions instead of replacing them
            lastPositions.add(position);
        }

        private void matchOne(Optional<Character> symbol)
        {
            int position = nextPosition++;
            symbolForPosition.put(position, symbol);

            if (firstPositions.isEmpty()) {
                firstPositions.add(position);
            }

            // link last positions to current position
            for (Integer lastPosition : lastPositions) {
                followPositions.put(lastPosition, position);
            }

            if (!foundFirstNonNullablePosition) {
                firstPositions.add(position);
                foundFirstNonNullablePosition = true;
            }

            lastPositions.clear();
            lastPositions.add(position);

            if (symbol.isPresent()) {
                alphabet.add(symbol.get());
            }
        }

        public void build()
        {
            // add transition to artificial accepting state
            matchOne(Optional.<Character>absent());

            Graph<DfaState, Optional<Character>> dfa = buildDfa();

            dfa = optimize(dfa);

//            String out = dfaToString(dfa, alphabet);

//            System.out.println(out);
        }


        private Graph<DfaState, Optional<Character>> buildDfa()
        {
            Graph<DfaState, Optional<Character>> dfa = new Graph<>();

            DfaState startingState = new DfaState(firstPositions, true, firstPositions.contains(Iterables.getOnlyElement(lastPositions)));
            dfa.addNode(startingState);

            Queue<DfaState> pending = new ArrayDeque<>();
            pending.add(startingState);

//            Set<Character> symbols = IterableTransformer.on(startingState.getPositions())
//                    .transform(forMap(symbolForPosition))
//                    .select(Optionals.isPresentPredicate())
//                    .transform(Optionals.<Character>optionalGetter())
//                    .set();

            while (!pending.isEmpty()) {
                DfaState state = pending.poll();

                SetMultimap<Optional<Character>, Integer> transitions = HashMultimap.create();

                for (int position : state.getPositions()) {
                    Optional<Character> symbol = symbolForPosition.get(position);

                    Set<Integer> follow = followPositions.get(position);

                    transitions.putAll(symbol, follow);

                    if (!symbol.isPresent()) { // is this a "match any"
                        for (Character character : alphabet) {
                            transitions.putAll(Optional.of(character), follow);
                        }
                    }
                }

                for (Map.Entry<Optional<Character>, Collection<Integer>> entry : transitions.asMap().entrySet()) {
                    Set<Integer> positions = ImmutableSet.copyOf(entry.getValue());

                    boolean isAccepting = positions.contains(Iterables.getOnlyElement(lastPositions));

                    DfaState targetState = new DfaState(positions, false, isAccepting);
                    if (!dfa.contains(targetState)) {
                        pending.add(targetState);
                        dfa.addNode(targetState);
                    }

                    dfa.addEdge(state, targetState, entry.getKey());
                }
            }

            // send all unmatched symbols to a dead state
            DfaState deadState = new DfaState(ImmutableSet.<Integer>of(), false, false);
            dfa.addNode(deadState);
            for (DfaState state : dfa.getNodes()) {
                for (Character character : alphabet) {
                    if (!dfa.hasOutgoingEdge(state, Optional.of(character))) {
                        dfa.addEdge(state, deadState, Optional.of(character));
                    }
                }
                if (!dfa.hasOutgoingEdge(state, Optional.<Character>absent())) {
                    dfa.addEdge(state, deadState, Optional.<Character>absent());
                }
            }

            return dfa;
        }

        private Graph<DfaState, Optional<Character>> optimize(Graph<DfaState, Optional<Character>> dfa)
        {
            System.out.println(dfaToString(dfa, alphabet));

            final Map<DfaState, Integer> groupByState = new HashMap<>();
            SetMultimap<Integer, DfaState> statesByGroup = HashMultimap.create();
            int nextGroupId = 0;

            List<Integer> workList = new ArrayList<>();
            Set<Integer> partition = new HashSet<>();

            // Given a DFA:
            //  Q = states,
            //  F = accepting states,
            //  ∂ = transitions
            //  ∑ = alphabet

            // worklist  W := { F, Q - F }
            // partition P := { F, Q - F }
            int acceptingGroup = nextGroupId++;
            int nonAcceptingGroup = nextGroupId++;

            for (DfaState state : dfa.getNodes()) {
                if (state.isAccepting()) {
                    groupByState.put(state, acceptingGroup);
                    statesByGroup.put(acceptingGroup, state);
                }
                else {
                    groupByState.put(state, nonAcceptingGroup);
                    statesByGroup.put(nonAcceptingGroup, state);
                }
            }
            workList.add(acceptingGroup);
            workList.add(nonAcceptingGroup);

            partition.add(acceptingGroup);
            partition.add(nonAcceptingGroup);

            // while W not empty
            while (!workList.isEmpty()) {
                // select and remove S from W (S is a set of states)
                int targetGroup = workList.remove(0);

                // foreach a in ∑
                for (Character character : alphabet) {
                    // I_a = ∂_a^-1(S) -- set of all states that can reach S on a
                    Set<DfaState> sources = new HashSet<>();

                    for (DfaState member : statesByGroup.get(targetGroup)) {
                        sources.addAll(dfa.getPredecessors(member, Optional.of(character)));
                    }

                    Set<Integer> newPartition = new HashSet<>();
                    // for each R in P such that intersection(R, I_a) != {} and R not contained in I_a
                    for (int candidateGroup : partition) {
                        Sets.SetView<DfaState> intersection = Sets.intersection(statesByGroup.get(candidateGroup), sources);
                        if (!intersection.isEmpty() && !sources.containsAll(statesByGroup.get(candidateGroup))) {
                            // partition R into R1 = intersection(R, I_a) and R2 = R - R1
                            Set<DfaState> first = ImmutableSet.copyOf(intersection);
                            Set<DfaState> second = ImmutableSet.copyOf(Sets.difference(statesByGroup.get(candidateGroup), first));

                            // replace R in P with R1 and R2
                            int firstId = nextGroupId++;
                            for (DfaState state : first) {
                                groupByState.put(state, firstId);
                                statesByGroup.put(firstId, state);
                            }

                            int secondId = nextGroupId++;
                            for (DfaState state : second) {
                                groupByState.put(state, secondId);
                                statesByGroup.put(secondId, state);
                            }

                            newPartition.add(firstId);
                            newPartition.add(secondId);


                            // if R is in W, replace R with R1 in W and add R2 to W
                            if (workList.contains(candidateGroup)) {
                                workList.remove((Object) candidateGroup);
                                workList.add(firstId);
                                workList.add(secondId);
                            }
                            else if (first.size() < second.size()) { // else if |R1| <= |R2|
                                // add R1 to W
                                workList.add(firstId);
                            }
                            else {
                                // add R2 to W
                                workList.add(secondId);
                            }
                        }
                        else {
                            newPartition.add(candidateGroup);
                        }
                    }

                    partition = newPartition;
                }
            }

            Map<Integer, DfaState> mergedStates = new HashMap<>();
            Graph<DfaState, Optional<Character>> optimized = new Graph<>();
            for (int groupId : partition) {
                boolean accepting = Iterables.any(statesByGroup.get(groupId), isAcceptingStatePredicate());
                boolean starting = Iterables.any(statesByGroup.get(groupId), isStartingStatePredicate());

                DfaState state = new DfaState(ImmutableSet.of(groupId), starting, accepting);
                optimized.addNode(state);
                mergedStates.put(groupId, state);
            }

            for (int fromGroup : partition) {
                for (DfaState from : statesByGroup.get(fromGroup)) {
                    for (Edge<DfaState, Optional<Character>> edge : dfa.getOutgoingEdges(from)) {
                        int toGroup = groupByState.get(edge.getTo());

                        optimized.addEdge(mergedStates.get(fromGroup), mergedStates.get(toGroup), edge.getType());
                    }
                }
            }
//
//            System.out.println(partition);
//            System.out.println(IterableTransformer.on(partition)
//                .transform(forMap(statesByGroup.asMap()))
//                .list());

            List<String> availableColors = ImmutableList.of("salmon2", "deepskyblue", "goldenrod2", "burlywood2",
                    "gold1", "greenyellow", "darkseagreen", "dodgerblue1", "thistle2", "darkolivegreen3", "chocolate", "turquoise3");
            
            final Map<Integer, String> colors = new HashMap<>();
            int index = 0;
            for (Integer groupId : partition) {
                if (index < availableColors.size()) {
                    colors.put(groupId, availableColors.get(index++));
                }
            }

            final String defaultTransition = "!(" + Joiner.on("|").join(alphabet) + ")";
            String out = GraphvizPrinter.toGraphviz(optimized, new Function<DfaState, Map<String, String>>()
                    {
                        @Override
                        public Map<String, String> apply(DfaState state)
                        {
                            ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
                            if (state.isAccepting()) {
                                builder.put("peripheries", "2");
                            }
                            if (state.isStart()) {
                                builder.put("shape", "\"rect\"");
                            }
                            builder.put("label", "\"" + state.getPositions().toString() + "\"");

                            String color = colors.get(groupByState.get(state));
                            if (color != null) {
                                builder.put("fillcolor", "\"" + color + "\"");
                                builder.put("style", "filled");
                            }
                            return builder.build();
                        }
                    },
                    new Function<Edge<DfaState, Optional<Character>>, Map<String, String>>()
                    {
                        @Override
                        public Map<String, String> apply(Edge<DfaState, Optional<Character>> edge)
                        {
                            ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
                            if (edge.getType().isPresent()) {
                                builder.put("label", "\"" + edge.getType().get() + "\"");
                            }
                            else {
                                builder.put("label", "\"" + defaultTransition + "\"");
                            }
                            return builder.build();
                        }
                    }
            );

            System.out.println(out);
            return null;
        }
    }

    private static class DfaState
    {
        private final Set<Integer> positions;
        private final boolean start;
        private final boolean accepting;

        private DfaState(Set<Integer> positions, boolean start, boolean accepting)
        {
            this.positions = positions;
            this.start = start;
            this.accepting = accepting;
        }

        public Set<Integer> getPositions()
        {
            return positions;
        }

        private boolean isStart()
        {
            return start;
        }

        private boolean isAccepting()
        {
            return accepting;
        }

        @Override
        public String toString()
        {
            return positions.toString();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            DfaState dfaState = (DfaState) o;

            if (!positions.equals(dfaState.positions)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            return positions.hashCode();
        }
    }

    private static Predicate<DfaState> isStartingStatePredicate()
    {
        return new Predicate<DfaState>()
        {
            @Override
            public boolean apply(DfaState input)
            {
                return input.isStart();
            }
        };
    }

    private static Predicate<DfaState> isAcceptingStatePredicate()
    {
        return new Predicate<DfaState>()
        {
            @Override
            public boolean apply(DfaState input)
            {
                return input.isAccepting();
            }
        };
    }

    public static void main(String[] args)
    {
//        LikePattern.compile("%a%b%c%", '\\');
//        LikePattern.compile("abcd%", '\\');
//        LikePattern.compile("%ab%cd%", '\\');
                LikePattern.compile("%abc_de%fg%", '\\');
    }

    private static String dfaToString(Graph<DfaState, Optional<Character>> dfa, Set<Character> symbols)
    {
        final String defaultTransition = "!(" + Joiner.on("|").join(symbols) + ")";
        return GraphvizPrinter.toGraphviz(dfa, new Function<DfaState, Map<String, String>>()
                {
                    @Override
                    public Map<String, String> apply(DfaState state)
                    {
                        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
                        if (state.isAccepting()) {
                            builder.put("peripheries", "2");
                        }
                        if (state.isStart()) {
                            builder.put("shape", "\"rect\"");
                        }
                        builder.put("label", "\"" + state.getPositions().toString() + "\"");
                        return builder.build();
                    }
                },
                new Function<Edge<DfaState, Optional<Character>>, Map<String, String>>()
                {
                    @Override
                    public Map<String, String> apply(Edge<DfaState, Optional<Character>> edge)
                    {
                        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
                        if (edge.getType().isPresent()) {
                            builder.put("label", "\"" + edge.getType().get() + "\"");
                        }
                        else {
                            builder.put("label", "\"" + defaultTransition + "\"");
                        }
                        return builder.build();
                    }
                }
        );
    }
}
