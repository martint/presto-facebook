package like;

import com.google.common.base.Joiner;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LikePattern
{
    public static void main(String[] args)
    {
        LikeNode node;

        node = new MatchAnyNode(1);
        node = new MatchZeroOrMoreNode(node);
        node = new ConcatenateNode(node, new MatchExactNode(2, 'l'));
        node = new ConcatenateNode(node, new MatchExactNode(3, 'y'));
        node = new ConcatenateNode(node, new MatchZeroOrMoreNode(new MatchAnyNode(4)));
        node = new ConcatenateNode(node, new MatchExactNode(5, 'l'));
        node = new ConcatenateNode(node, new MatchExactNode(6, 'y'));
        node = new ConcatenateNode(node, new MatchZeroOrMoreNode(new MatchAnyNode(7)));
        node = new ConcatenateNode(node, new MatchExactNode(8, '#'));


        Map<Integer, Set<Integer>> followPositions = new HashMap<>();
        computeFollowPositions(node, followPositions);

        System.out.println(Joiner.on("\n").withKeyValueSeparator(":").join(followPositions));
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
            positions = new HashSet<>();
            output.put(position, positions);
        }

        positions.addAll(followPositions);
    }
}
