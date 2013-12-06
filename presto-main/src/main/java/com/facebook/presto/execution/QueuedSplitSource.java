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
package com.facebook.presto.execution;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.SplitSource;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class QueuedSplitSource
        implements SplitSource
{
    private static final Split END_OF_STREAM = new Split()
    {
        @Override
        public boolean isRemotelyAccessible()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<HostAddress> getAddresses()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getInfo()
        {
            throw new UnsupportedOperationException();
        }
    };

    private final String dataSourceName;
    private final BlockingQueue<Split> splitQueue;

    public QueuedSplitSource(@Nullable String dataSourceName, int maxQueueSize)
    {
        this.dataSourceName = dataSourceName;
        this.splitQueue = new ArrayBlockingQueue<>(maxQueueSize);
    }

    @Override
    @Nullable
    public String getDataSourceName()
    {
        return dataSourceName;
    }

    @Override
    public boolean isFinished()
    {
        return splitQueue.peek() == END_OF_STREAM;
    }

    public void close()
    {
        // todo wrong
        splitQueue.add(END_OF_STREAM);
    }

    @Override
    public List<Split> getNextBatch(int maxSize)
            throws InterruptedException
    {
        List<Split> splits = new ArrayList<>(maxSize);

        splits.add(splitQueue.take());
        splitQueue.drainTo(splits, maxSize - 1);
        if (splits.get(splits.size() - 1) == QueuedSplitSource.END_OF_STREAM) {
            splits.remove(splits.size() - 1);
        }
        return splits;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("dataSourceName", dataSourceName)
                .toString();
    }

    public static class SplitQueue
    {
        private final AtomicBoolean closed = new AtomicBoolean();
        private final BlockingQueue<Split> splitQueue;

        public SplitQueue(BlockingQueue<Split> splitQueue)
        {
            this.splitQueue = splitQueue;
        }

        public SplitQueue(int queueSize)
        {
            splitQueue = new ArrayBlockingQueue<>(queueSize);
        }

        public void addSplits(Split... splits)
                throws InterruptedException
        {
            addSplits(ImmutableList.copyOf(splits));
        }

        public void addSplits(Iterable<? extends Split> splits)
                throws InterruptedException
        {
            for (Split split : splits) {
                addSingleSplit(split);
            }
        }

        private void addSingleSplit(Split split)
                throws InterruptedException
        {
            while (!closed.get()) {
                if (splitQueue.offer(split, 250, TimeUnit.MILLISECONDS)) {
                    return;
                }
            }
        }

        public void noMoreSplits()
                throws InterruptedException
        {
            if (closed.compareAndSet(false, true)) {
                splitQueue.put(END_OF_STREAM);
            }
        }
    }
}
