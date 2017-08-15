package com.facebook.presto.type.setdigest;

import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateMetadata;

@AccumulatorStateMetadata(stateSerializerClass = SetDigestStateSerializer.class, stateFactoryClass = SetDigestStateFactory.class)
public interface SetDigestState
        extends AccumulatorState
{
    SetDigest getDigest();

    void setDigest(SetDigest value);
}
