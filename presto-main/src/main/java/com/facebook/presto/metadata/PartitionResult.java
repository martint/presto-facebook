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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;

public class PartitionResult
{
    private final List<Partition> partitions;
    private final TupleDomain undeterminedTupleDomain;

    public PartitionResult(String connectorId, ConnectorPartitionResult connectorPartitionResult)
    {
        Preconditions.checkNotNull(connectorId, "connectorId is null");
        Preconditions.checkNotNull(connectorPartitionResult, "connectorPartitionResult is null");

        partitions = Lists.transform(connectorPartitionResult.getPartitions(), partition -> new Partition(connectorId, partition));
        undeterminedTupleDomain = connectorPartitionResult.getUndeterminedTupleDomain();
    }

    public List<Partition> getPartitions()
    {
        return partitions;
    }

    public TupleDomain getUndeterminedTupleDomain()
    {
        return undeterminedTupleDomain;
    }
}
