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

import com.facebook.presto.hive.util.SuspendingExecutor;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SplitQueue
{
    private static final ConnectorSplit FINISHED_MARKER = new ConnectorSplit()
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

    private final BlockingQueue<ConnectorSplit> queue = new LinkedBlockingQueue<>();
    private final AtomicInteger outstandingSplitCount = new AtomicInteger();
    private final AtomicReference<Throwable> throwable = new AtomicReference<>();
    private final int maxOutstandingSplits;
    private final SuspendingExecutor suspendingExecutor;

    public int getOutstandingSplitCount()
    {
        return outstandingSplitCount.get();
    }

    void addToQueue(Iterable<? extends ConnectorSplit> splits)
    {
        for (ConnectorSplit split : splits) {
            add(split);
        }
    }

    public void add(ConnectorSplit split)
    {
        if (throwable.get() == null) {
            queue.add(split);
            if (outstandingSplitCount.incrementAndGet() >= maxOutstandingSplits) {
                suspendingExecutor.suspend();
            }
        }
    }

    public void finish()
    {
        if (throwable.get() == null) {
            queue.add(FINISHED_MARKER);
        }
    }

    public void fail(Throwable e)
    {
        // only record the first error message
        if (throwable.compareAndSet(null, e)) {
            // add the finish marker
            queue.add(FINISHED_MARKER);

            // no need to process any more jobs
            suspendingExecutor.suspend();
        }
    }

    @Override
    public List<ConnectorSplit> getNextBatch(int maxSize)
            throws InterruptedException
    {
        // wait for at least one split and then take as may extra splits as possible
        // if an error has been registered, the take will succeed immediately because
        // will be at least one finished marker in the queue
        List<ConnectorSplit> splits = new ArrayList<>(maxSize);
        splits.add(queue.take());
        queue.drainTo(splits, maxSize - 1);

        // check if we got the finished marker in our list
        int finishedIndex = splits.indexOf(FINISHED_MARKER);
        if (finishedIndex >= 0) {
            // add the finish marker back to the queue so future callers will not block indefinitely
            queue.add(FINISHED_MARKER);
            // drop all splits after the finish marker (this shouldn't happen in a normal exit, but be safe)
            splits = splits.subList(0, finishedIndex);
        }

        // Before returning, check if there is a registered failure.
        // If so, we want to throw the error, instead of returning because the scheduler can block
        // while scheduling splits and wait for work to finish before continuing.  In this case,
        // we want to end the query as soon as possible and abort the work
        if (throwable.get() != null) {
            throw propagatePrestoException(throwable.get());
        }

        // decrement the outstanding split count by the number of splits we took
        if (outstandingSplitCount.addAndGet(-splits.size()) < maxOutstandingSplits) {
            // we are below the low water mark (and there isn't a failure) so resume scanning hdfs
            suspendingExecutor.resume();
        }

        return splits;
    }

    @Override
    public void close()
    {
        // TODO
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public boolean isFinished()
    {
        // the finished marker must be checked before checking the throwable
        // to avoid a race with the fail method
        boolean isFinished = queue.peek() == FINISHED_MARKER;
        if (throwable.get() != null) {
            throw propagatePrestoException(throwable.get());
        }
        return isFinished;
    }

    private RuntimeException propagatePrestoException(Throwable throwable)
    {
        if (throwable instanceof PrestoException) {
            throw (PrestoException) throwable;
        }
        if (throwable instanceof FileNotFoundException) {
            throw new PrestoException(HiveErrorCode.HIVE_FILE_NOT_FOUND.toErrorCode(), throwable);
        }
        throw new PrestoException(HiveErrorCode.HIVE_UNKNOWN_ERROR.toErrorCode(), throwable);
    }


}
