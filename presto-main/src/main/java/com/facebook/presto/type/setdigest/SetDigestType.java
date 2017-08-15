package com.facebook.presto.type.setdigest;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.AbstractVariableWidthType;
import com.facebook.presto.spi.type.SqlVarbinary;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;

// Layout is <size>:<digest>, where
//   size: is an int describing the length of the set digest bytes
//   digest: is the serialized digest
public class SetDigestType
        extends AbstractVariableWidthType
{
    public static final SetDigestType SET_DIGEST = new SetDigestType();
    public static final String NAME = "SetDigest";

    private SetDigestType()
    {
        super(parseTypeSignature(NAME), Slice.class);
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        return new SqlVarbinary(block.getSlice(position, 0, block.getSliceLength(position)).getBytes());
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        return block.getSlice(position, 0, block.getSliceLength(position));
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            block.writeBytesTo(position, 0, block.getSliceLength(position), blockBuilder);
            blockBuilder.closeEntry();
        }
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        writeSlice(blockBuilder, value, 0, value.length());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        blockBuilder.writeBytes(value, offset, length).closeEntry();
    }
}
