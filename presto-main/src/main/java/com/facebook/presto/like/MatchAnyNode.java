package com.facebook.presto.like;

import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

public class MatchAnyNode
    extends LikeNode
{
    private final int position;

    public MatchAnyNode(int position)
    {
        this.position = position;
    }

    public int getPosition()
    {
        return position;
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
