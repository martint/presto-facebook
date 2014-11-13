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
package com.facebook.presto.sql.newplanner.optimizer;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class PhysicalConstraints
{
    public enum GlobalPartitioning {
        UNPARTITIONED,
        PARTITIONED,
        REPLICATED
    }

    private final Optional<GlobalPartitioning> partitioningConstraint;
    private final Optional<List<Integer>> partitioningColumns;

    public PhysicalConstraints(Optional<GlobalPartitioning> partitioningConstraint, Optional<List<Integer>> partitioningColumns)
    {
        this.partitioningConstraint = checkNotNull(partitioningConstraint, "partitioningConstraint is null");
        this.partitioningColumns = checkNotNull(partitioningColumns, "partitioningColumns is null");
    }

    public Optional<GlobalPartitioning> getPartitioningConstraint()
    {
        return partitioningConstraint;
    }

    public static PhysicalConstraints partitioned(List<Integer> columns)
    {
        return new PhysicalConstraints(Optional.of(GlobalPartitioning.PARTITIONED), Optional.of(columns));
    }

    public static PhysicalConstraints partitionedAny()
    {
        return new PhysicalConstraints(Optional.of(GlobalPartitioning.PARTITIONED), Optional.<List<Integer>>absent());
    }

    public static PhysicalConstraints unpartitioned()
    {
        return new PhysicalConstraints(Optional.of(GlobalPartitioning.UNPARTITIONED), Optional.<List<Integer>>absent());
    }

    public static PhysicalConstraints replicated()
    {
        return new PhysicalConstraints(Optional.of(GlobalPartitioning.REPLICATED), Optional.<List<Integer>>absent());
    }

    public static PhysicalConstraints any()
    {
        return new PhysicalConstraints(Optional.<GlobalPartitioning>absent(), Optional.<List<Integer>>absent());
    }

    public boolean hasPartitioningConstraint()
    {
        return partitioningConstraint.isPresent();
    }

    public Optional<List<Integer>> getPartitioningColumns()
    {
        return partitioningColumns;
    }

    // global:
    //   partitioning scheme

    // local:
    //   sorting columns
    //   grouping columns

    // domain
    // uniqueness
    // functional dependencies

    // maybe?
    //   estimated cardinality
    //   histograms

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();

        if (partitioningConstraint.isPresent()) {
            switch (partitioningConstraint.get()) {
                case UNPARTITIONED:
                    builder.append("unpartitioned");
                    break;
                case PARTITIONED:
                    builder.append("partitioned: ");
                    if (partitioningColumns.isPresent()) {
                        builder.append(partitioningColumns.get());
                    }
                    else {
                        builder.append("<any>");
                    }
                    break;
                case REPLICATED:
                    builder.append("replicated");
                    break;
            }
        }
        else {
            builder.append("<any>");
        }

        return builder.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PhysicalConstraints that = (PhysicalConstraints) o;

        if (!partitioningColumns.equals(that.partitioningColumns)) {
            return false;
        }
        if (!partitioningConstraint.equals(that.partitioningConstraint)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = partitioningConstraint.hashCode();
        result = 31 * result + partitioningColumns.hashCode();
        return result;
    }
}
