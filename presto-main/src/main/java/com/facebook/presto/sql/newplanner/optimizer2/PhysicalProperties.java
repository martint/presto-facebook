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
package com.facebook.presto.sql.newplanner.optimizer2;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class PhysicalProperties
{
    private final Optional<List<Integer>> partitioningColumns;

    private PhysicalProperties(Optional<List<Integer>> partitioningColumns)
    {
        this.partitioningColumns = partitioningColumns;
    }

    public boolean isPartitioned()
    {
        return partitioningColumns.isPresent();
    }

    public List<Integer> getPartitioningColumns()
    {
        return partitioningColumns.get();
    }

    public static PhysicalProperties unpartitioned()
    {
        return new PhysicalProperties(Optional.<List<Integer>>absent());
    }

    public static PhysicalProperties randomPartitioned()
    {
        return new PhysicalProperties(Optional.<List<Integer>>of(ImmutableList.<Integer>of()));
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();

        if (!partitioningColumns.isPresent()) {
            builder.append("partitioned: " + partitioningColumns.get());
        }
        else {
            builder.append("unpartitioned");
        }

        return builder.toString();
    }

}
