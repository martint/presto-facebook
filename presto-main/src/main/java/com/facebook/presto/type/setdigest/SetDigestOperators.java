package com.facebook.presto.type.setdigest;

import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.spi.function.OperatorType.CAST;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;

public final class SetDigestOperators
{
    private SetDigestOperators()
    {
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice castToBinary(@SqlType(SetDigestType.NAME) Slice slice)
    {
        return slice;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.HYPER_LOG_LOG)
    public static Slice castToHyperLogLog(@SqlType(SetDigestType.NAME) Slice slice)
    {
        checkArgument(slice.getByte(0) == 1, "CAST only works on version 1 of SetDigest format");
        int hllLength = slice.getInt(SIZE_OF_BYTE);
        return Slices.wrappedBuffer(slice.getBytes(SIZE_OF_BYTE + SIZE_OF_INT, hllLength));
    }
}
