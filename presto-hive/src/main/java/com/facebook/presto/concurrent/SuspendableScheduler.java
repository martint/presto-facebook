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
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFutureTask;

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;

/**
 * Executes a given number of ResumableTasks at a time from the provided Iterable.
 * A task can request to be suspended by returning ControlResult.SUSPEND.
 * When a task is complete, it should indicate so by returning ControlResult.FINISHED.
 *
 * Tasks can be resumed via the resume() method.
 *
 * The scheduler can be shutdown by calling close(). Any running tasks will be interrupted
 * and should return control as soon as possible. Once they finish executing, the close()
 * method will be called to allow for resource cleanup. Suspended tasks will also receive
 * a call to close(). Tasks in the input Iterable that haven't been run yet won't receive
 * a call to close().
 */
public class SuspendableScheduler
        implements Closeable
{
    private final Semaphore permits;
    private final Iterable<ResumableTask> pending;
    private final ExecutorService executor;
    private final int concurrency;

    @GuardedBy("this")
    private final Set<ResumableTask> toClose = new HashSet<>();

    @GuardedBy("this")
    private final Set<ResumableTask> suspended = new HashSet<>();

    @GuardedBy("this")
    private final Set<ListenableFutureTask<?>> running = new HashSet<>();

    private volatile boolean done;

    public SuspendableScheduler(Iterable<ResumableTask> tasks, ExecutorService executor, int concurrency)
    {
        this.pending = tasks;
        this.executor = executor;
        this.concurrency = concurrency;

        permits = new Semaphore(concurrency);
    }

    public void run()
            throws InterruptedException
    {
        for (ResumableTask task : pending) {
            if (!waitForPermit()) {
                // we're done!
                return;
            }

            run(task);
        }

        // wait for active tasks to complete
        for (int i = 0; i < concurrency; i++) {
            if (!waitForPermit()) {
                return;
            }
        }

        boolean interrupted = false;
        Set<ResumableTask> toClose;
        synchronized (this) {
            try {
                Futures.allAsList(running).get();
            }
            catch (InterruptedException e) {
                interrupted = true;
            }
            catch (ExecutionException ignore) {
            }

            toClose = ImmutableSet.copyOf(this.toClose);
        }

        for (ResumableTask task : toClose) {
            close(task);
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
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
        synchronized (this) {
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

        synchronized (this) {
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

        synchronized (this) {
            for (FutureTask<?> future : running) {
                future.cancel(true);
            }
        }

        permits.release(concurrency);
    }

    private void run(final ResumableTask task)
    {
        final ListenableFutureTask<ControlResult> future = ListenableFutureTask.create(task);

        Futures.addCallback(future, new FutureCallback<ControlResult>() {
            @Override
            public void onSuccess(ControlResult state)
            {
                switch (state) {
                    case FINISH:
                        close(task);
                        permits.release();
                        break;
                    case SUSPEND:
                        synchronized (SuspendableScheduler.this) {
                            suspended.add(task);
                            running.remove(future);
                        }
                        break;
                }
            }

            @Override
            public void onFailure(Throwable t)
            {
                close(task);

                // TODO: record failure
            }
        });

        synchronized (this) {
            if (done) {
                // someone may have called close() so don't don't add the task
                // to the running list and don't even execute it.
                return;
            }
            toClose.add(task);
            running.add(future);
        }

        // it's ok to submit this now. If someone calls close between the block above and
        // this call, the task will be registered in the running list and close() will
        // interrupt it
        executor.submit(future);
    }

    private void close(ResumableTask task)
    {
        try {
            task.close();
        }
        catch (IOException ignored) {
        }

        synchronized (this) {
            toClose.remove(task);
        }
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

        public ControlResult call()
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
