package com.facebook.presto.like;

import com.facebook.presto.graph.Edge;
import com.facebook.presto.graph.Graph;
import com.facebook.presto.graph.GraphvizPrinter;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
            // add transition to artificial terminal state
            matchOne(Optional.<Character>absent());

            Graph<DfaState, Optional<Character>> dfa = buildDfa();

            dfa = optimize(dfa);

            String out = dfaToString(dfa, alphabet);

            System.out.println(out);
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

                    boolean isTerminal = positions.contains(Iterables.getOnlyElement(lastPositions));

                    DfaState targetState = new DfaState(positions, false, isTerminal);
                    if (!dfa.contains(targetState)) {
                        pending.add(targetState);
                        dfa.addNode(targetState);
                    }

                    dfa.addEdge(state, targetState, entry.getKey());
                }

            }
            return dfa;
        }

        private Graph<DfaState, Optional<Character>> optimize(Graph<DfaState, Optional<Character>> dfa)
        {
            System.out.println(dfaToString(dfa, alphabet));

            // merge redundant states
            // remove unnecessary edges ("else" edges to itself, "else" + specific edges to same target)
            Partition partition = initialPartition(dfa);

            while (true) {
                Partition newPartition = new Partition();
                for (Collection<DfaState> group : partition.groups()) {
                    // partition group by each symbol
                    for (Character symbol : alphabet) {
                        Map<Integer, DfaState> byTargetGroup = new HashMap<>();
                        for (DfaState state : group) {

                        }
//                        boolean needsNewGroup = true;
//                        for (Map.Entry<DfaState, Integer> entry : newPartition.states().entrySet()) {
//                            if (shouldBeInSameGroup(entry.getKey(), state, dfa, partition)) {
//                                newPartition.put(state, entry.getValue());
//                                needsNewGroup = false;
//                            }
//                            break;
//                        }
//
//                        // no existing compatible group found. Create a new one
//                        if (needsNewGroup) {
//                            newPartition.put(state, newPartition.newGroup());
//                        }
                    }
                }

                if (partition.size() == newPartition.size()) {
                    break;
                }

                partition = newPartition;
            }

            return null;
        }
    }


    private static boolean shouldBeInSameGroup(DfaState state1, DfaState state2, Graph<DfaState, Optional<Character>> dfa, Partition partition)
    {
        // assumption: all states have a default transition

        Map<Optional<Character>, Integer> state1Targets = computeTargetGroupBySymbol(state1, dfa, partition);
        Map<Optional<Character>, Integer> state2Targets = computeTargetGroupBySymbol(state2, dfa, partition);

        // verify target groups for symbols in state1 but not in state2 all point to the default target for state2
        {
            Iterable<Integer> targets = Iterables.transform(Sets.difference(state1Targets.keySet(), state2Targets.keySet()), forMap(state1Targets));
            if (!Iterables.all(targets, equalTo(state2Targets.get(Optional.<Character>absent())))) {
                return false;
            }
        }

        // check in the opposite direction
        {
            Iterable<Integer> targets = Iterables.transform(Sets.difference(state2Targets.keySet(), state1Targets.keySet()), forMap(state2Targets));
            if (!Iterables.all(targets, equalTo(state1Targets.get(Optional.<Character>absent())))) {
                return false;
            }
        }

        // verify that common symbols have the same target (including default transition)
        for (Optional<Character> symbol : Sets.intersection(state1Targets.keySet(), state2Targets.keySet())) {
            if (state1Targets.get(symbol) != state2Targets.get(symbol)) {
                return false;
            }
        }

        return true;
    }

    private static Map<Optional<Character>, Integer> computeTargetGroupBySymbol(DfaState state1, Graph<DfaState, Optional<Character>> dfa, Partition partition)
    {
        Set<Edge<DfaState, Optional<Character>>> edges = dfa.getOutgoingEdges(state1);
        Map<Optional<Character>, Integer> targetsBySymbol = new HashMap<>();
        for (Edge<DfaState, Optional<Character>> entry : edges) {
            int targetGroup = partition.states().get(entry.getTo());
            targetsBySymbol.put(entry.getType(), targetGroup);
        }
        return targetsBySymbol;
    }

    private static Partition initialPartition(Graph<DfaState, ?> dfa)
    {
        Partition partition = new Partition();

        int terminal = partition.newGroup();
        int nonTerminal = partition.newGroup();

        for (DfaState state : dfa.getNodes()) {
            if (state.isTerminal()) {
                partition.put(state, terminal);
            }
            else {
                partition.put(state, nonTerminal);
            }
        }

        return partition;
    }

    private static class Partition
    {
        private int nextGroup;
        private final Map<DfaState, Integer> byState = new HashMap<>();
        private final SetMultimap<Integer, DfaState> byGroup = HashMultimap.create();

        public int size()
        {
            return byGroup.keySet().size();
        }

        public int newGroup()
        {
            return nextGroup++;
        }

        public void put(DfaState state, int group)
        {
            byState.put(state, group);
            byGroup.put(group, state);
        }

        public Collection<Collection<DfaState>> groups()
        {
            return byGroup.asMap().values();
        }

        public Map<DfaState, Integer> states()
        {
            return byState;
        }
    }

    private static class DfaState
    {
        private final Set<Integer> positions;
        private final boolean start;
        private final boolean terminal;

        private DfaState(Set<Integer> positions, boolean start, boolean terminal)
        {
            this.positions = positions;
            this.start = start;
            this.terminal = terminal;
        }

        public Set<Integer> getPositions()
        {
            return positions;
        }

        private boolean isStart()
        {
            return start;
        }

        private boolean isTerminal()
        {
            return terminal;
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


    public static void main(String[] args)
    {
        LikePattern.compile("%ab%cd%", '\\');
        //        LikePattern.compile("%abc_de%fg%", '\\');
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
                        if (state.isTerminal()) {
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
