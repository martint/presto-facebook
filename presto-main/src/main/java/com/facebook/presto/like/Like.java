package com.facebook.presto.like;

import com.facebook.presto.graph.Edge;
import com.facebook.presto.graph.Graph;
import com.facebook.presto.graph.GraphvizPrinter;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public class Like
{
    public static LikeMatcher compile(String pattern, char escapeChar)
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

        Graph<DfaState, Optional<Character>> dfa = builder.build();

        return matcherFor(dfa);
    }

    private static LikeMatcher matcherFor(Graph<DfaState, Optional<Character>> dfa)
    {
        int numberOfStates = dfa.getNodeCount();

        int startingState = -1;
        int acceptingState = -1;
        int[] offsets = new int[numberOfStates];
        int[] numberOfTransitions = new int[numberOfStates];
        int[] elseTargets = new int[numberOfStates];

        int nonDefaultTransitionCount = 0;
        Map<DfaState, Integer> states = new HashMap<>();
        for (DfaState state : dfa.getNodes()) {
            int stateIndex = states.size();
            states.put(state, stateIndex);

            if (state.isAccepting()) {
                Preconditions.checkState(acceptingState == -1, "multiple accepting states not supported");
                acceptingState = stateIndex;
            }

            if (state.isStart()) {
                Preconditions.checkState(startingState == -1, "cannot have more than one starting state");
                startingState = stateIndex;
            }

            offsets[stateIndex] = nonDefaultTransitionCount;
            for (Edge<DfaState, Optional<Character>> edge : dfa.getOutgoingEdges(state)) {
                if (edge.getType().isPresent()) {
                    nonDefaultTransitionCount++;
                }
            }
        }


        char symbols[] = new char[nonDefaultTransitionCount];
        int targets[] = new int[nonDefaultTransitionCount];

        for (Map.Entry<DfaState, Integer> entry : states.entrySet()) {
            DfaState state = entry.getKey();
            int stateIndex = entry.getValue();

            int offset = offsets[stateIndex];
            int transitions = 0;

            // initialize default transition goes to self (in case the node has no edges)
            elseTargets[stateIndex] = stateIndex;

            for (Edge<DfaState, Optional<Character>> edge : dfa.getOutgoingEdges(state)) {
                int toIndex = states.get(edge.getTo());
                if (edge.getType().isPresent()) {
                    symbols[offset + transitions] = edge.getType().get();
                    targets[offset + transitions] = toIndex;
                    transitions++;
                }
                else {
                    elseTargets[stateIndex] = toIndex;
                }
            }
            numberOfTransitions[stateIndex] = transitions;
        }

        return new LikeMatcher(states.size(), startingState, acceptingState, numberOfTransitions, elseTargets, offsets, symbols, targets);
    }


    private static class DfaBuilder
    {
        private int nextPosition;

        private final Set<Integer> positionsWithMatchAny = new HashSet<>();
        private final SetMultimap<Character, Integer> positionsForSymbol = HashMultimap.create();
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

            positionsWithMatchAny.add(position);

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
                positionsForSymbol.put(symbol.get(), position);
            }
            else {
                positionsWithMatchAny.add(position);
            }
        }

        public Graph<DfaState, Optional<Character>> build()
        {
            // add transition to artificial accepting state
            matchOne(Optional.<Character>absent());

            Graph<DfaState, Optional<Character>> dfa = buildDfa();

            //                        System.out.println(dfaToString(dfa, alphabet));

            dfa = optimize(dfa);

            //            String out = dfaToString(dfa, alphabet);
            //            System.out.println(out);

            return dfa;
        }


        private Graph<DfaState, Optional<Character>> buildDfa()
        {
            Graph<DfaState, Optional<Character>> dfa = new Graph<>();

            DfaState startingState = new DfaState(firstPositions, true, firstPositions.contains(Iterables.getOnlyElement(lastPositions)));
            dfa.addNode(startingState);

            Queue<DfaState> pending = new ArrayDeque<>();
            pending.add(startingState);

            while (!pending.isEmpty()) {
                DfaState state = pending.poll();

                Set<Integer> elseFollow = new HashSet<>();
                for (int position : state.getPositions()) {
                    if (positionsWithMatchAny.contains(position)) {
                        elseFollow.addAll(followPositions.get(position));
                    }
                }

                // add "else" transition. If 'follow' is empty, it represents the dead state
                boolean isAccepting = elseFollow.contains(Iterables.getOnlyElement(lastPositions));

                DfaState targetState = new DfaState(elseFollow, false, isAccepting);
                if (!dfa.contains(targetState)) {
                    pending.add(targetState);
                    dfa.addNode(targetState);
                }

                dfa.addEdge(state, targetState, Optional.<Character>absent());

                // add a transition for each named character
                for (Character character : alphabet) {
                    Set<Integer> targetPositions = new HashSet<>();
                    targetPositions.addAll(elseFollow);

                    for (int position : state.getPositions()) {
                        Optional<Character> symbol = symbolForPosition.get(position);
                        if (symbol.isPresent() && symbol.get() == character) {
                            targetPositions.addAll(followPositions.get(position));
                        }
                    }

                    isAccepting = targetPositions.contains(Iterables.getOnlyElement(lastPositions));
                    targetState = new DfaState(targetPositions, false, isAccepting);
                    if (!dfa.contains(targetState)) {
                        pending.add(targetState);
                        dfa.addNode(targetState);
                    }

                    dfa.addEdge(state, targetState, Optional.of(character));
                }
            }

            return dfa;
        }

        private Graph<DfaState, Optional<Character>> optimize(Graph<DfaState, Optional<Character>> dfa)
        {
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

                List<Optional<Character>> all = new ArrayList<>();
                for (Character character : alphabet) {
                    all.add(Optional.of(character));
                }
                all.add(Optional.<Character>absent());

                // foreach a in ∑
                for (Optional<Character> character : all) {
                    // I_a = ∂_a^-1(S) -- set of all states that can reach S on a
                    Set<DfaState> sources = new HashSet<>();

                    for (DfaState member : statesByGroup.get(targetGroup)) {
                        sources.addAll(dfa.getPredecessors(member, character));
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

            // merge redundant states
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

            // remove redundant edges
            for (DfaState state : optimized.getNodes()) {
                DfaState defaultTarget = optimized.getSuccessor(state, Optional.<Character>absent());
                if (defaultTarget == null) {
                    defaultTarget = state;
                }

                // 1. remove any non-default transition with the same target as the default transition (or itself if default transition is missing)
                for (Edge<DfaState, Optional<Character>> edge : optimized.getOutgoingEdges(state)) {
                    if (edge.getType().isPresent() && edge.getTo() == defaultTarget) {
                        optimized.removeEdge(edge.getFrom(), edge.getTo(), edge.getType());
                    }
                }

                if (optimized.getSuccessor(state, Optional.<Character>absent()) == state) {
                    // 2. if only has a 'default' transition to itself, remove it
                    optimized.removeEdge(state, state, Optional.<Character>absent());
                }
            }

            return optimized;
            ////
            ////            System.out.println(partition);
            ////            System.out.println(IterableTransformer.on(partition)
            ////                .transform(forMap(statesByGroup.asMap()))
            ////                .list());
            //
            //            List<String> availableColors = ImmutableList.of("salmon2", "deepskyblue", "goldenrod2", "burlywood2", "gold1", "greenyellow", "darkseagreen", "dodgerblue1", "thistle2", "darkolivegreen3", "chocolate", "turquoise3");
            //
            //            final Map<DfaState, String> colors = new HashMap<>();
            //            int index = 0;
            //
            ////            for (int groupId : partition) {
            ////                if (index < availableColors.size()) {
            ////                    colors.put(groupId, availableColors.get(index++));
            ////                }
            ////            }
            //
            ////            final String defaultTransition = "!(" + Joiner.on("|").join(alphabet) + ")";
            //            final String defaultTransition = "";
            //            String out = GraphvizPrinter.toGraphviz(optimized, new Function<DfaState, Map<String, String>>()
            //                    {
            //                        @Override
            //                        public Map<String, String> apply(DfaState state)
            //                        {
            //                            ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
            //                            if (state.isAccepting()) {
            //                                builder.put("peripheries", "2");
            //                            }
            //                            if (state.isStart()) {
            //                                builder.put("shape", "\"rect\"");
            //                            }
            //                            builder.put("label", "\"" + state.getPositions().toString() + "\"");
            //
            //                            String color = colors.get(groupByState.get(state));
            //                            if (color != null) {
            //                                builder.put("fillcolor", "\"" + color + "\"");
            //                                builder.put("style", "filled");
            //                            }
            //                            return builder.build();
            //                        }
            //                    },
            //                    new Function<Edge<DfaState, Optional<Character>>, Map<String, String>>()
            //                    {
            //                        @Override
            //                        public Map<String, String> apply(Edge<DfaState, Optional<Character>> edge)
            //                        {
            //                            ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
            //                            if (edge.getType().isPresent()) {
            //                                builder.put("label", "\"" + edge.getType().get() + "\"");
            //                            }
            //                            else {
            //                                builder.put("label", "\"" + defaultTransition + "\"");
            //                            }
            //                            return builder.build();
            //                        }
            //                    }
            //            );
            //
            //            System.out.println(out);
            //            return null;
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
        //        LikePattern.compile("abc%", '\\');
        //        LikePattern.compile("%ly%ly%", '\\');
        //        LikePattern.compile("%a%b%c%", '\\');
        //        LikePattern.compile("abcd%", '\\');
        //        LikePattern.compile("%ab%cd%", '\\');
        LikeMatcher matcher = Like.compile("abc%", '\\');
        Preconditions.checkState(matcher.matches("abc"));
    }

    private static String dfaToString(Graph<DfaState, Optional<Character>> dfa, Set<Character> symbols)
    {
        //        final String defaultTransition = "!(" + Joiner.on("|").join(symbols) + ")";
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
                            builder.put("label", "\"!\"");
                        }
                        return builder.build();
                    }
                }
        );
    }
}
