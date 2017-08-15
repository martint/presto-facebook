package com.facebook.presto.type.setdigest;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.Type;

import static com.facebook.presto.type.setdigest.SetDigestType.SET_DIGEST;

public class SetDigestStateSerializer
        implements AccumulatorStateSerializer<SetDigestState>
{
    @Override
    public Type getSerializedType()
    {
        return SET_DIGEST;
    }

    @Override
    public void serialize(SetDigestState state, BlockBuilder out)
    {
        if (state.getDigest() == null) {
            out.appendNull();
        }
        else {
            SET_DIGEST.writeSlice(out, state.getDigest().serialize());
        }
    }

    @Override
    public void deserialize(Block block, int index, SetDigestState state)
    {
        state.setDigest(SetDigest.newInstance(block.getSlice(index, 0, block.getSliceLength(index))));
    }
}
