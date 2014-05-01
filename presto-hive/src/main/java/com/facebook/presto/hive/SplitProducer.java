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

import com.facebook.presto.spi.HostAddress;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.airlift.units.DataSize;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;

import static com.google.common.base.Preconditions.checkState;

public class SplitProducer
        implements Runnable, Closeable
{
    private final ClassLoader classLoader;
    private final SplitQueue queue;
    private final Executor executor;

    private final Table table;
    private final Iterable<String> names;
    private final Iterable<Partition> partitions;
    private final int concurrency;
    private final DataSize maxSplitSize;

    public SplitProducer(
            ClassLoader classLoader,
            Executor executor,
            Table table,
            Iterable<String> names,
            Iterable<Partition> partitions,
            int concurrency,
            DataSize maxSplitSize)
    {
        this.classLoader = classLoader;
        this.table = table;
        this.names = names;
        this.partitions = partitions;
        this.concurrency = concurrency;
        this.maxSplitSize = maxSplitSize;

        this.executor = executor;

        this.queue = new SplitQueue();
    }

    public SplitQueue getQueue()
    {
        return queue;
    }

    public void run()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Iterator<String> nameIterator = names.iterator();
            Iterator<Partition> partitionIterator = partitions.iterator();
            while (nameIterator.hasNext() && partitionIterator.hasNext()) {

                String partitionName = nameIterator.next();
                Partition partition = partitionIterator.next();

            }

            checkState(!nameIterator.hasNext() && !partitionIterator.hasNext(), "different number of partitions and partition names!");
        }
    }

    private static List<HostAddress> toHostAddress(String[] hosts)
    {
        ImmutableList.Builder<HostAddress> builder = ImmutableList.builder();
        for (String host : hosts) {
            builder.add(HostAddress.fromString(host));
        }
        return builder.build();
    }

    @Override
    public void close()
            throws IOException
    {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
