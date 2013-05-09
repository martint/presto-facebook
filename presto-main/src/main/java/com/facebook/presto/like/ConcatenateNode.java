package com.facebook.presto.like;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

public class ConcatenateNode
        extends LikeNode
{
    private final LikeNode left;
    private final LikeNode right;

    public ConcatenateNode(LikeNode left, LikeNode right)
    {
        this.left = left;
        this.right = right;
    }

    public LikeNode getLeft()
    {
        return left;
    }

    public LikeNode getRight()
    {
        return right;
    }

    @Override
    public boolean isNullable()
    {
        return left.isNullable() && right.isNullable();
    }

    @Override
    public Set<Integer> firstPositions()
    {
        if (left.isNullable()) {
            return ImmutableSet.copyOf(Sets.union(left.firstPositions(), right.firstPositions()));
        }
        else {
            return left.firstPositions();
        }
    }

    @Override
    public Set<Integer> lastPositions()
    {
        if (right.isNullable()) {
            return ImmutableSet.copyOf(Sets.union(left.lastPositions(), right.lastPositions()));
        }
        else {
            return right.lastPositions();
        }
    }

    @Override
    public Set<Integer> computeFollowPositions(Map<Integer, LikeNode> nodesByPosition)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
