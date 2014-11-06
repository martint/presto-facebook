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
    private final boolean hasPartitioningConstraint;
    private final Optional<List<Integer>> partitioningColumns;

    public PhysicalConstraints(boolean hasPartitioningConstraint, Optional<List<Integer>> partitioningColumns)
    {
        this.hasPartitioningConstraint = hasPartitioningConstraint;
        this.partitioningColumns = checkNotNull(partitioningColumns, "partitioningColumns is null");
    }

    public static PhysicalConstraints partitioned(List<Integer> columns)
    {
        return new PhysicalConstraints(true, Optional.of(columns));
    }

    public static PhysicalConstraints unpartitioned()
    {
        return new PhysicalConstraints(true, Optional.<List<Integer>>absent());
    }

    public static PhysicalConstraints any()
    {
        return new PhysicalConstraints(false, Optional.<List<Integer>>absent());
    }

    public boolean hasPartitioningConstraint()
    {
        return hasPartitioningConstraint;
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

        if (!partitioningColumns.isPresent()) {
            builder.append("unpartitioned");
        }
        else {
            builder.append("partitioned: " + partitioningColumns.get());
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

        return true;
    }

    @Override
    public int hashCode()
    {
        return partitioningColumns.hashCode();
    }
}
