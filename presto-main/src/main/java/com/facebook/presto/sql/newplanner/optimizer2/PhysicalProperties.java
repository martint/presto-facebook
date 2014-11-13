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

import com.facebook.presto.sql.newplanner.optimizer.PhysicalConstraints;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.sql.newplanner.optimizer.PhysicalConstraints.GlobalPartitioning.PARTITIONED;
import static com.facebook.presto.sql.newplanner.optimizer.PhysicalConstraints.GlobalPartitioning.REPLICATED;
import static com.facebook.presto.sql.newplanner.optimizer.PhysicalConstraints.GlobalPartitioning.UNPARTITIONED;

public class PhysicalProperties
{
    private final PhysicalConstraints.GlobalPartitioning globalPartitioning;
    private final Optional<List<Integer>> partitioningColumns;

    private PhysicalProperties(PhysicalConstraints.GlobalPartitioning globalPartitioning, Optional<List<Integer>> partitioningColumns)
    {
        this.globalPartitioning = globalPartitioning;
        this.partitioningColumns = partitioningColumns;
    }

    public PhysicalConstraints.GlobalPartitioning getGlobalPartitioning()
    {
        return globalPartitioning;
    }

    public boolean isPartitioned()
    {
        return globalPartitioning == PARTITIONED;
    }

    public List<Integer> getPartitioningColumns()
    {
        return partitioningColumns.get();
    }

    public static PhysicalProperties unpartitioned()
    {
        return new PhysicalProperties(UNPARTITIONED, Optional.<List<Integer>>absent());
    }

    public static PhysicalProperties randomPartitioned()
    {
        return new PhysicalProperties(PARTITIONED, Optional.<List<Integer>>of(ImmutableList.<Integer>of()));
    }

    public static PhysicalProperties partitioned(List<Integer> columns)
    {
        return new PhysicalProperties(PARTITIONED, Optional.of(columns));
    }

    public static PhysicalProperties replicated()
    {
        return new PhysicalProperties(REPLICATED, Optional.<List<Integer>>absent());
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();

        switch (globalPartitioning) {
            case UNPARTITIONED:
                builder.append("unpartitioned");
                break;
            case PARTITIONED:
                builder.append("partitioned: " + partitioningColumns.get());
                break;
            case REPLICATED:
                builder.append("replicated");
                break;
        }

        return builder.toString();
    }

}
