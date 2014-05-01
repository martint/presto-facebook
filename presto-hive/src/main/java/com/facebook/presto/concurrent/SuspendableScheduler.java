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
package com.facebook.presto.concurrent;

import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;

public class SuspendableScheduler
        implements Closeable
{
    private final Semaphore permits;
    private final Iterable<ResumableTask> pending;
    private final ExecutorService executor;
    private final int concurrency;

    @GuardedBy("this")
    private final Set<ResumableTask> suspended = new HashSet<>();

    private volatile boolean done;

    public SuspendableScheduler(Iterable<ResumableTask> tasks, ExecutorService executor, int concurrency)
    {
        this.pending = tasks;
        this.executor = executor;
        this.concurrency = concurrency;

        permits = new Semaphore(concurrency);
    }

    public void run()
    {
        for (final ResumableTask task : pending) {
            if (!waitForPermit()) {
                // TODO: what to do with suspended and running tasks?
                // we're done!
                return;
            }

            run(task);
        }

        // wait for active tasks to complete
        for (int i = 0; i < concurrency; i++) {
            if (!waitForPermit()) {
                // TODO: what to do with suspended and running tasks?
                return;
            }
        }
    }

    private boolean waitForPermit()
    {
        try {
            permits.acquire();

            // we might be seeing the release from close(), so check if we're done
            if (done) {
                return false;
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }

        return true;
    }

    private void cleanup()
    {
        synchronized (suspended) {
            for (ResumableTask task : suspended) {
                try {
                    task.close();
                }
                catch (IOException ignored) {
                }
            }
            suspended.clear();
        }
    }

    public void resume()
    {
        checkState(!done);

        synchronized (suspended) {
            for (ResumableTask task : suspended) {
                run(task);
            }
            suspended.clear();
        }
    }

    @Override
    public void close()
    {
        done = true;
        // TODO: cancel tasks?
        permits.release(concurrency);
    }

    private void run(final ResumableTask task)
    {
        executor.submit(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    ControlResult state = task.run();

                    switch (state) {
                        case FINISH:
                            permits.release();
                            break;
                        case SUSPEND:
                            synchronized (suspended) {
                                suspended.add(task);
                            }
                            break;
                    }
                }
                catch (Throwable e) {
                    // record failure
                }
            }
        });
    }

    public static void main(String[] args)
            throws InterruptedException
    {
        List<ResumableTask> tasks = ImmutableList.<ResumableTask>of(
                new CountdownTask("a", 3),
                new CountdownTask("b", 5),
                new CountdownTask("c", 4)
        );

        ExecutorService executor = Executors.newCachedThreadPool();
        final SuspendableScheduler scheduler = new SuspendableScheduler(tasks, executor, 2);

        ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutor.scheduleWithFixedDelay(new Runnable()
        {
            @Override
            public void run()
            {
                System.out.println("resuming");
                scheduler.resume();
            }
        }, 1, 1, TimeUnit.SECONDS);

        scheduler.run();

        System.out.println("done");
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
        Thread.sleep(100);
    }

    private static class CountdownTask
            implements ResumableTask
    {
        private final String name;
        private int count;

        private CountdownTask(String name, int count)
        {
            this.name = name;
            this.count = count;
        }

        public ControlResult run()
        {
            if (count > 0) {
                System.out.println(name + ":" + count);
                count--;
                return ControlResult.SUSPEND;
            }

            return ControlResult.FINISH;
        }

        @Override
        public void close()
                throws IOException
        {
        }
    }
}
