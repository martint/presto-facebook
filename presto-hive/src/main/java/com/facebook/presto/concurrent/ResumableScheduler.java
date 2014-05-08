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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFutureTask;

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
 * <p/>
 * Tasks can be resumed via the resume() method.
 * <p/>
 * The scheduler can be shutdown by calling close(). Any running tasks will be interrupted
 * and should return control as soon as possible. Once they finish executing, the close()
 * method will be called to allow for resource cleanup.
 * <p/>
 * Every task that had it's call() method executed at least once is guaranteed to see
 * a call to close(). Note that it is possible for tasks that are in the process of being
 * scheduled for execution to see a call to close() if this scheduler is shut down prematurely
 */
public class ResumableScheduler
        implements Closeable
{
    private final Semaphore permits;
    private final Iterable<ResumableTask> pending;
    private final ExecutorService executor;
    private final int concurrency;

    @GuardedBy("this")
    private final Set<ResumableTask> suspended = new HashSet<>();

    // keep track of running tasks to be able to interrupt them
    @GuardedBy("this")
    private final Set<ListenableFutureTask<?>> running = new HashSet<>();

    private volatile boolean done;
    private volatile Thread schedulerThread;
    private volatile Throwable exception;

    public ResumableScheduler(Iterable<ResumableTask> tasks, ExecutorService executor, int concurrency)
    {
        this.pending = tasks;
        this.executor = executor;
        this.concurrency = concurrency;

        permits = new Semaphore(concurrency);
    }

    public void run()
    {
        synchronized (this) {
            // in case someone called close before we started running
            if (done) {
                return;
            }
            schedulerThread = Thread.currentThread();
        }

        try {
            for (ResumableTask task : pending) {
                waitForPermit();
                run(task);
            }

            // wait for active tasks to complete
            for (int i = 0; i < concurrency; i++) {
                waitForPermit();
            }

            // The only way to get here is if all tasks complete successfully
            synchronized (this) {
                checkState(suspended.isEmpty());
                checkState(running.isEmpty());
            }
        }
        catch (InterruptedException e) {
            synchronized (this) {
                for (ResumableTask task : suspended) {
                    close(task);
                }
                suspended.clear();
            }
            Thread.currentThread().interrupt();
        }
    }

    private void waitForPermit()
            throws InterruptedException
    {
        permits.acquire();
        if (exception != null) {
            throw Throwables.propagate(exception);
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
    public synchronized void close()
    {
        done = true;

        if (schedulerThread != null) {
            schedulerThread.interrupt();
        }

        // interrupt all running tasks. They will be closed by
        // their registered callbacks
        for (FutureTask<?> future : running) {
            future.cancel(true);
        }
    }

    private void run(final ResumableTask task)
    {
        final ListenableFutureTask<ControlResult> future = ListenableFutureTask.create(task);

        Futures.addCallback(future, new FutureCallback<ControlResult>()
        {
            @Override
            public void onSuccess(ControlResult state)
            {
                boolean needsClose;

                synchronized (ResumableScheduler.this) {
                    running.remove(future);

                    needsClose = (state == ControlResult.FINISH || done);

                    switch (state) {
                        case FINISH:
                            permits.release();
                            break;
                        case SUSPEND:
                            suspended.add(task);
                            break;
                    }
                }

                if (needsClose) {
                    close(task);
                }
            }

            @Override
            public void onFailure(Throwable throwable)
            {
                synchronized (ResumableScheduler.this) {
                    exception = throwable;
                    running.remove(future);
                    permits.release();
                }

                close(task);
            }
        });

        synchronized (this) {
            if (done) {
                // someone may have called close() so don't don't add the task
                // to the running list and don't even execute it.
                return;
            }
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
    }

    /////////////////////////////////////////////////////////////////////////////////////
//    public static void main(String[] args)
//    {
//        ExecutorService executor = Executors.newFixedThreadPool(1);
//
//        executor.execute(new Runnable()
//        {
//            @Override
//            public void run()
//            {
//                try {
//                    Thread.currentThread().join();
//                }
//                catch (InterruptedException e) {
//                    Thread.currentThread().interrupt();
//                }
//            }
//        });
//
//        ListenableFutureTask<Void> task = ListenableFutureTask.create(new Callable<Void>()
//        {
//            @Override
//            public Void call()
//                    throws Exception
//            {
//                System.out.println("task");
//                return null;
//            }
//        });
//
//        Futures.addCallback(task, new FutureCallback<Void>()
//        {
//            @Override
//            public void onSuccess(@Nullable Void result)
//            {
//                System.out.println("success");
//            }
//
//            @Override
//            public void onFailure(Throwable t)
//            {
//                System.out.println("failure");
//                t.printStackTrace();
//            }
//        });
//
//        executor.submit(task);
//
//        task.cancel(true);
//    }

    public static void main(String[] args)
            throws InterruptedException
    {
        List<ResumableTask> tasks = ImmutableList.<ResumableTask>of(
                new CountdownTask("a", 3),
                new CountdownTask("b", 5),
                new CountdownTask("c", 4)
        );

        ExecutorService executor = Executors.newCachedThreadPool();
        final ResumableScheduler scheduler = new ResumableScheduler(tasks, executor, 2);

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
