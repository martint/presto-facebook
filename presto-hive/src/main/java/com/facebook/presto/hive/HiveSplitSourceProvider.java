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
package com.facebook.presto.hive;

import com.facebook.presto.hive.util.BoundedExecutor;
import com.facebook.presto.hive.util.SuspendingExecutor;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.Session;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.stats.TimeStat;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.facebook.presto.hadoop.HadoopFileStatus.isFile;
import static com.facebook.presto.hive.HiveBucketing.HiveBucket;
import static com.facebook.presto.hive.HiveType.getSupportedHiveType;
import static com.facebook.presto.hive.HiveUtil.convertNativeHiveType;
import static com.facebook.presto.hive.HiveUtil.getInputFormat;
import static com.facebook.presto.hive.HiveUtil.isSplittable;
import static com.facebook.presto.hive.UnpartitionedPartition.isUnpartitioned;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

class HiveSplitSourceProvider
{

    private final String connectorId;
    private final Table table;
    private final Iterable<String> partitionNames;
    private final Iterable<Partition> partitions;
    private final Optional<HiveBucket> bucket;
    private final int maxOutstandingSplits;
    private final int maxThreads;
    private final HdfsEnvironment hdfsEnvironment;
    private final NamenodeStats namenodeStats;
    private final DirectoryLister directoryLister;
    private final ExecutorService executor;
    private final ClassLoader classLoader;
    private final DataSize maxSplitSize;
    private final int maxPartitionBatchSize;
    private final Session session;

    HiveSplitSourceProvider(String connectorId,
            Table table,
            Iterable<String> partitionNames,
            Iterable<Partition> partitions,
            Optional<HiveBucket> bucket,
            DataSize maxSplitSize,
            int maxOutstandingSplits,
            int maxThreads,
            HdfsEnvironment hdfsEnvironment,
            NamenodeStats namenodeStats,
            DirectoryLister directoryLister,
            ExecutorService executor,
            int maxPartitionBatchSize,
            Session session)
    {
        this.connectorId = connectorId;
        this.table = table;
        this.partitionNames = partitionNames;
        this.partitions = partitions;
        this.bucket = bucket;
        this.maxSplitSize = maxSplitSize;
        this.maxPartitionBatchSize = maxPartitionBatchSize;
        this.maxOutstandingSplits = maxOutstandingSplits;
        this.maxThreads = maxThreads;
        this.hdfsEnvironment = hdfsEnvironment;
        this.namenodeStats = namenodeStats;
        this.directoryLister = directoryLister;
        this.executor = executor;
        this.session = session;
        this.classLoader = Thread.currentThread().getContextClassLoader();
    }

    public ConnectorSplitSource get()
    {
        SplitProducer producer = new SplitProducer(classLoader, executor, table, partitionNames, partitions, maxThreads, maxSplitSize);
        Future<?> producerFuture = executor.submit(producer);

        return new HiveSplitSource(connectorId, producer.getQueue(), producerFuture);
    }

    @VisibleForTesting
    static class HiveSplitSource
            implements ConnectorSplitSource
    {
        private final String connectorId;
        private final SplitQueue queue;
        private final Future<?> producerFuture;

        HiveSplitSource(String connectorId, SplitQueue queue, Future<?> producerFuture)
        {
            this.connectorId = connectorId;
            this.queue = queue;
            this.producerFuture = producerFuture;
        }

        @Override
        public String getDataSourceName()
        {
            return connectorId;
        }

        @Override
        public List<ConnectorSplit> getNextBatch(int maxSize)
                throws InterruptedException
        {
            return queue.getNextBatch(maxSize);
        }

        @Override
        public void close()
        {
            producerFuture.cancel(true);
            queue.finish();
        }

        @Override
        public boolean isFinished()
        {
            return queue.isFinished();
        }
    }

    private static List<HivePartitionKey> getPartitionKeys(Table table, Partition partition)
    {
        if (isUnpartitioned(partition)) {
            return ImmutableList.of();
        }
        ImmutableList.Builder<HivePartitionKey> partitionKeys = ImmutableList.builder();
        List<FieldSchema> keys = table.getPartitionKeys();
        List<String> values = partition.getValues();
        checkArgument(keys.size() == values.size(), "Expected %s partition key values, but got %s", keys.size(), values.size());
        for (int i = 0; i < keys.size(); i++) {
            String name = keys.get(i).getName();
            PrimitiveObjectInspector.PrimitiveCategory primitiveCategory = convertNativeHiveType(keys.get(i).getType());
            HiveType hiveType = getSupportedHiveType(primitiveCategory);
            String value = values.get(i);
            checkNotNull(value, "partition key value cannot be null for field: %s", name);
            partitionKeys.add(new HivePartitionKey(name, hiveType, value));
        }
        return partitionKeys.build();
    }

    private static Properties getPartitionSchema(Table table, Partition partition)
    {
        if (isUnpartitioned(partition)) {
            return MetaStoreUtils.getTableMetadata(table);
        }
        return MetaStoreUtils.getSchema(partition, table);
    }

    private static String getPartitionLocation(Table table, Partition partition)
    {
        if (isUnpartitioned(partition)) {
            return table.getSd().getLocation();
        }
        return partition.getSd().getLocation();
    }
}
