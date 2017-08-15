package com.facebook.presto.type.setdigest;

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;

@AggregationFunction("make_set_digest")
public final class BuildSetDigestAggregation
{
    private static final SetDigestStateSerializer SERIALIZER = new SetDigestStateSerializer();

    private BuildSetDigestAggregation() {}

    @InputFunction
    public static void input(SetDigestState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        if (state.getDigest() == null) {
            state.setDigest(new SetDigest());
        }
        state.getDigest().add(value);
    }

    @CombineFunction
    public static void combine(SetDigestState state, SetDigestState otherState)
    {
        if (state.getDigest() == null) {
            SetDigest copy = new SetDigest();
            copy.mergeWith(otherState.getDigest());
            state.setDigest(copy);
        }
        else {
            state.getDigest().mergeWith(otherState.getDigest());
        }
    }

    @OutputFunction(SetDigestType.NAME)
    public static void output(SetDigestState state, BlockBuilder out)
    {
        SERIALIZER.serialize(state, out);
    }
}
