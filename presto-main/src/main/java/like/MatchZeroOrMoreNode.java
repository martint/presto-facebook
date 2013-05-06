package like;

import java.util.Map;
import java.util.Set;

public class MatchZeroOrMoreNode
    extends LikeNode
{
    private LikeNode child;

    public MatchZeroOrMoreNode(LikeNode child)
    {
        this.child = child;
    }

    @Override
    public boolean isNullable()
    {
        return true;
    }

    @Override
    public Set<Integer> firstPositions()
    {
        return child.firstPositions();
    }

    @Override
    public Set<Integer> lastPositions()
    {
        return child.lastPositions();
    }

    @Override
    public Set<Integer> computeFollowPositions(Map<Integer, LikeNode> nodesByPosition)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
