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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;

public class TestResumableScheduler
{
    private ExecutorService executor;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        executor = Executors.newCachedThreadPool();
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        executor.shutdownNow();
    }

    @Test
    public void testBasic()
            throws Exception
    {
        AtomicInteger accumulator = new AtomicInteger();
        List<ResumableTask> tasks = ImmutableList.<ResumableTask>of(
                new CounterTask1(0, accumulator),
                new CounterTask1(0, accumulator),
                new CounterTask1(0, accumulator),
                new CounterTask1(0, accumulator));

        ResumableScheduler scheduler = new ResumableScheduler(tasks, executor, 1);

        scheduler.run();

        assertEquals(accumulator.get(), 4);
    }

    @Test
    public void testConcurrency()
            throws Exception
    {
        final int maxConcurrency = 2;
        final AtomicInteger running = new AtomicInteger();
        ImmutableList<ResumableTask> tasks = ImmutableList.<ResumableTask>of(
                new SleepingTask(running, maxConcurrency),
                new SleepingTask(running, maxConcurrency),
                new SleepingTask(running, maxConcurrency),
                new SleepingTask(running, maxConcurrency),
                new SleepingTask(running, maxConcurrency),
                new SleepingTask(running, maxConcurrency));

        ResumableScheduler scheduler = new ResumableScheduler(tasks, executor, 2);
        scheduler.run();
    }

    /*
        Ensures that a task that asks to be suspended but doesn't return immediately doesn't get executed
        simultaneously by two threads
     */
    @Test
    public void testMisbehavedSuspendWithResume()
            throws Exception
    {
        final Phaser afterSuspend = new Phaser(2);

        List<ResumableTask> tasks = ImmutableList.<ResumableTask>of(new ResumableTask()
        {
            private volatile boolean running;
            private volatile int run = 0;

            @Override
            public ControlResult call(TaskControl control)
                    throws InterruptedException
            {
                Preconditions.checkState(!running, "Task is already running");

                if (run > 0) {
                    return control.finish();
                }
                run++;

                try {
                    running = true;

                    // suspend but don't return immediately to allow controller
                    // to resume us before we return
                    control.suspend();

                    // trigger resume
                    afterSuspend.arriveAndAwaitAdvance();

                    Thread.sleep(10);
                    return ControlResult.SUSPEND;
                }
                finally {
                    running = false;
                }
            }

            @Override
            public void close()
                    throws IOException
            {
            }
        });

        ResumableScheduler scheduler = new ResumableScheduler(tasks, executor, 1);
        Future<?> future = executor.submit(scheduler);

        afterSuspend.arriveAndAwaitAdvance();
        scheduler.resume();

        future.get();
    }

    //    @Test
//    public void testResume()
//            throws Exception
//    {
//        Phaser phaser = new Phaser(2);
//
//        List<ResumableTask> tasks = ImmutableList.<ResumableTask>of(
//                new CounterTask1(0, accumulator),
//                new CounterTask1(0, accumulator),
//                new CounterTask1(0, accumulator),
//                new CounterTask1(0, accumulator));
//
//        final ResumableScheduler scheduler = new ResumableScheduler(tasks, executor, 1);
//
//        executor.submit(new Runnable()
//        {
//            @Override
//            public void run()
//            {
//                scheduler.run();
//            }
//        });
//
//
//        assertEquals(accumulator.get(), 4);
//    }
//

    private static class SleepingTask
        implements ResumableTask
    {
        private final AtomicInteger running;
        private final int maxConcurrency;

        public SleepingTask(AtomicInteger running, int maxConcurrency)
        {
            this.running = running;
            this.maxConcurrency = maxConcurrency;
        }

        @Override
        public ControlResult call(TaskControl control)
        throws Exception
        {
            Preconditions.checkState(running.incrementAndGet() <= maxConcurrency);
            Thread.sleep(100);
            running.decrementAndGet();
            return control.finish();
        }

        @Override
        public void close()
        throws IOException
        {
        }
    }

    private static class CounterTask1
            implements ResumableTask
    {
        private int count;
        private final AtomicInteger accumulator;

        public CounterTask1(int count, AtomicInteger accumulator)
        {
            this.count = count;
            this.accumulator = accumulator;
        }

        @Override
        public ControlResult call(TaskControl control)
        {
            accumulator.incrementAndGet();
            if (count == 0) {
                return control.finish();
            }

            count--;
            return control.suspend();
        }

        @Override
        public void close()
                throws IOException
        {
        }
    }

//    private static class CounterTask2
//            implements ResumableTask
//    {
//        private int count;
//        private final Phaser phaser;
//
//        public CounterTask2(int count, Phaser phaser)
//        {
//            this.count = count;
//            this.phaser = phaser;
//        }
//
//        @Override
//        public ControlResult call()
//                throws Exception
//        {
//            if (count == 0) {
//                return ControlResult.FINISH;
//            }
//
//            count--;
//            phaser.arrive();
//            return ControlResult.SUSPEND;
//        }
//
//        @Override
//        public void close()
//                throws IOException
//        {
//        }
//    }
}
