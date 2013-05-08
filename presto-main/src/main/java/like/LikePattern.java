package like;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;

public class LikePattern
{
    public static void main(String[] args)
    {
        LikeNode node;

//                node = new ConcatenateNode(
//                        new ConcatenateNode(
//                                new ConcatenateNode(
//                                        new ConcatenateNode(
//                                                new ConcatenateNode(
//                                                        new ConcatenateNode(
//                                                                new ConcatenateNode(
//                                                                        new MatchZeroOrMoreNode(
//                                                                                new MatchAnyNode(1)),
//                                                                        new MatchExactNode(2, 'a')),
//                                                                new MatchExactNode(3, 'b')),
//                                                        new MatchZeroOrMoreNode(new MatchAnyNode(4))),
//                                                new MatchExactNode(5, 'a')),
//                                        new MatchExactNode(6, 'b')),
//                                new MatchZeroOrMoreNode(new MatchAnyNode(7))),
//                        new MatchExactNode(8, '#'));
//
        node = new ConcatenateNode(
                new ConcatenateNode(
                        new ConcatenateNode(
                                new ConcatenateNode(
                                        new ConcatenateNode(
                                                new ConcatenateNode(
                                                        new ConcatenateNode(
                                                                new MatchZeroOrMoreNode(
                                                                        new MatchAnyNode(0)),
                                                                new MatchExactNode(1, 's')),
                                                        new MatchExactNode(2, 'l')),
                                                new MatchExactNode(3, 'y')),
                                        new MatchExactNode(4, 'l')),
                                new MatchExactNode(5, 'y')),
                        new MatchZeroOrMoreNode(new MatchAnyNode(6))),
                new MatchExactNode(7, '#'));
        //

        /*
         [1] -(a)-> [2, 3]
         [2, 3] -(null)-> [2, 3]
         [2, 3] ->(b)-> [2, 3, 4]
         */
        //        node =
        //                new ConcatenateNode(
        //                        new ConcatenateNode(
        //                                new ConcatenateNode(
        //                                        new MatchZeroOrMoreNode(
        //                                                new MatchAnyNode(1)),
        //                                        new MatchExactNode(2, 'a')),
        //                                new MatchExactNode(3, 'b')),
        //                        new MatchExactNode(4, '#'));
        //

        Map<Integer, LikeNode> byPosition = new HashMap<>();
        computeByPosition(node, byPosition);

        Map<Integer, Set<Integer>> followPositions = new HashMap<>();
        computeFollowPositions(node, followPositions);

        System.out.println(Joiner.on("\n").withKeyValueSeparator(":").join(followPositions));
        System.out.println();

        Set<Set<Integer>> states = new HashSet<>();
        Queue<Set<Integer>> unmarked = new ArrayDeque<>();

        unmarked.add(node.firstPositions());

        System.out.println("digraph dfa {");
        System.out.println("rankdir = LR;");
        while (!unmarked.isEmpty()) {
            Set<Integer> state = unmarked.poll();
            states.add(state);

            SetMultimap<Character, Integer> transitions = HashMultimap.create();

            Set<Character> specificChars = new TreeSet<>();
            for (Integer position : state) {
                LikeNode likeNode = byPosition.get(position);

                if (likeNode instanceof MatchExactNode) {
                    specificChars.add(((MatchExactNode) likeNode).getCharacter());
                }
            }

            for (int position : state) {
                LikeNode likeNode = byPosition.get(position);

                if (likeNode instanceof MatchExactNode) {
                    Set<Integer> follow = followPositions.get(position);
                    if (follow == null) {
                        follow = ImmutableSet.of();
                    }

                    char character = ((MatchExactNode) likeNode).getCharacter();
                    transitions.putAll(character, follow);
                }
                else if (likeNode instanceof MatchAnyNode) {
                    Set<Integer> follow = followPositions.get(position);
                    if (follow == null) {
                        follow = ImmutableSet.of();
                    }

                    transitions.putAll(null, follow);
                    for (Character specificChar : specificChars) {
                        transitions.putAll(specificChar, follow);
                    }
                }
                else {
                    throw new UnsupportedOperationException("not yet implemented: " + likeNode.getClass().getName());
                }
            }

            for (Map.Entry<Character, Collection<Integer>> entry : transitions.asMap().entrySet()) {
                Set<Integer> targetState = (Set<Integer>) entry.getValue();
                if (!states.contains(entry.getValue())) {
                    unmarked.add(targetState);
                }

                String from = Joiner.on(",").join(state);
                String to = Joiner.on(",").join(targetState);

                if (entry.getKey() == null) {
                    System.out.println(String.format("\"%s\" -> \"%s\" [label=\"else\"];", from, to));
                }
                else {
                    System.out.println(String.format("\"%s\" -> \"%s\" [label=\"%s\"];", from, to, entry.getKey()));
                }

//                System.out.println(state + " -(" + entry.getKey() + ")-> " + targetState);

                states.add(targetState);
            }

        }
        System.out.println("}");
    }

    private static void computeByPosition(LikeNode node, Map<Integer, LikeNode> output)
    {
        if (node instanceof MatchAnyNode) {
            output.put(((MatchAnyNode) node).getPosition(), node);
        }
        else if (node instanceof MatchExactNode) {
            output.put(((MatchExactNode) node).getPosition(), node);
        }
        else if (node instanceof MatchZeroOrMoreNode) {
            computeByPosition(((MatchZeroOrMoreNode) node).getChild(), output);
        }
        else if (node instanceof ConcatenateNode) {
            computeByPosition(((ConcatenateNode) node).getLeft(), output);
            computeByPosition(((ConcatenateNode) node).getRight(), output);
        }
    }

    private static void computeFollowPositions(LikeNode node, Map<Integer, Set<Integer>> output)
    {
        if (node instanceof ConcatenateNode) {
            LikeNode left = ((ConcatenateNode) node).getLeft();
            LikeNode right = ((ConcatenateNode) node).getRight();

            computeFollowPositions(left, output);
            computeFollowPositions(right, output);

            for (Integer leftLastPositions : left.lastPositions()) {
                addAll(output, leftLastPositions, right.firstPositions());
            }
        }
        else if (node instanceof MatchZeroOrMoreNode) {
            for (Integer lastPosition : node.lastPositions()) {
                addAll(output, lastPosition, node.firstPositions());
            }
        }
    }

    private static void addAll(Map<Integer, Set<Integer>> output, int position, Set<Integer> followPositions)
    {
        Set<Integer> positions = output.get(position);
        if (positions == null) {
            positions = new TreeSet<>();
            output.put(position, positions);
        }

        positions.addAll(followPositions);
    }
}
