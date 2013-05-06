package like;

import java.util.Map;
import java.util.Set;

public abstract class LikeNode
{
    public abstract boolean isNullable();
    public abstract Set<Integer> firstPositions();
    public abstract Set<Integer> lastPositions();

    public abstract Set<Integer> computeFollowPositions(Map<Integer, LikeNode> nodesByPosition);
}
