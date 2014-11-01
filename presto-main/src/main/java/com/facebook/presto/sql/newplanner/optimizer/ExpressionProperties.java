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

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class ExpressionProperties
{
    public static final ExpressionProperties UNPARTITIONED = new ExpressionProperties(Optional.<List<Integer>>absent());
    public static final ExpressionProperties RANDOM_PARTITION = new ExpressionProperties(Optional.<List<Integer>>of(ImmutableList.<Integer>of()));

    private final Optional<List<Integer>> partitioningColumns;

    private ExpressionProperties(Optional<List<Integer>> partitioningColumns)
    {
        this.partitioningColumns = partitioningColumns;
    }

    public static ExpressionProperties partitioned(List<Integer> columns)
    {
        return new ExpressionProperties(Optional.of(columns));
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
}
