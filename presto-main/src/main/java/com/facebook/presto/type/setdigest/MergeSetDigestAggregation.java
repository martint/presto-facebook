package com.facebook.presto.type.setdigest;

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

import static com.facebook.presto.type.setdigest.SetDigestType.SET_DIGEST;

@AggregationFunction("merge_set_digest")
public final class MergeSetDigestAggregation
{
    private MergeSetDigestAggregation() {}

    @InputFunction
    public static void input(SetDigestState state, @SqlType(SetDigestType.NAME) Slice value)
    {
        SetDigest instance = SetDigest.newInstance(value);
        merge(state, instance);
    }

    @CombineFunction
    public static void combine(SetDigestState state, SetDigestState otherState)
    {
        merge(state, otherState.getDigest());
    }

    private static void merge(SetDigestState state, SetDigest instance)
    {
        if (state.getDigest() == null) {
            state.setDigest(instance);
        }
        else {
            state.getDigest().mergeWith(instance);
        }
    }

    @OutputFunction(SetDigestType.NAME)
    public static void output(SetDigestState state, BlockBuilder out)
    {
        if (state.getDigest() == null) {
            out.appendNull();
        }
        else {
            SET_DIGEST.writeSlice(out, state.getDigest().serialize());
        }
    }
}
