/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;

public class GroupByIdBlock
        extends UncompressedBlock
{
    private static final int ENTRY_SIZE = SINGLE_LONG.getFixedSize();
    private final long maxGroupId;
    private final Slice slice;

    public GroupByIdBlock(long maxGroupId, Slice slice)
    {
        super(slice.length() / ENTRY_SIZE, SINGLE_LONG, slice);
        this.maxGroupId = maxGroupId;
        this.slice = slice;
    }

    public long getMaxGroupId()
    {
        return maxGroupId;
    }

    public long getGroupId(int position)
    {
        int entryOffset = position * ENTRY_SIZE;
        Preconditions.checkState(position >= 0 && entryOffset + ENTRY_SIZE <= slice.length(), "position is not valid");
        return slice.getLong(entryOffset + SIZE_OF_BYTE);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("maxGroupId", maxGroupId)
                .add("positionCount", getPositionCount())
                .toString();
    }
}
