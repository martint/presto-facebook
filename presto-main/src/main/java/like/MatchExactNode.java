package like;

import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

public class MatchExactNode
    extends LikeNode
{
    private final int position;
    private final char character;

    public MatchExactNode(int position, char character)
    {
        this.position = position;
        this.character = character;
    }

    @Override
    public boolean isNullable()
    {
        return false;
    }

    @Override
    public Set<Integer> firstPositions()
    {
        return ImmutableSet.of(position);
    }

    @Override
    public Set<Integer> lastPositions()
    {
        return ImmutableSet.of(position);
    }

    @Override
    public Set<Integer> computeFollowPositions(Map<Integer, LikeNode> nodesByPosition)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
