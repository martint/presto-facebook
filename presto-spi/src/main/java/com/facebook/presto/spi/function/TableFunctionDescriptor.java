package com.facebook.presto.spi.function;

import com.facebook.presto.spi.ColumnMetadata;

import java.util.List;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class TableFunctionDescriptor
{
    private final byte[] handle;
    private final List<Integer> inputColumns;
    private final List<ColumnMetadata> outputColumns;

    public TableFunctionDescriptor(byte[] handle, List<Integer> inputColumns, List<ColumnMetadata> outputColumns)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.inputColumns = unmodifiableList(requireNonNull(inputColumns, "inputColumns is null"));;
        this.outputColumns = unmodifiableList(requireNonNull(outputColumns, "outputColumns is null"));
    }

    public byte[] getHandle()
    {
        return handle;
    }

    public List<Integer> getInputColumns()
    {
        return inputColumns;
    }

    public List<ColumnMetadata> getOutputColumns()
    {
        return outputColumns;
    }
}
