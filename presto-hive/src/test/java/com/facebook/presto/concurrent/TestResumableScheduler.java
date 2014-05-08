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
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    public void testResume()
            throws Exception
    {
        Phaser phaser = new Phaser(2);

        List<ResumableTask> tasks = ImmutableList.<ResumableTask>of(
                new CounterTask1(0, accumulator),
                new CounterTask1(0, accumulator),
                new CounterTask1(0, accumulator),
                new CounterTask1(0, accumulator));

        final ResumableScheduler scheduler = new ResumableScheduler(tasks, executor, 1);

        executor.submit(new Runnable()
        {
            @Override
            public void run()
            {
                scheduler.run();
            }
        });


        assertEquals(accumulator.get(), 4);
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
        public ControlResult call()
                throws Exception
        {
            accumulator.incrementAndGet();
            if (count == 0) {
                return ControlResult.FINISH;
            }

            count--;
            return ControlResult.SUSPEND;
        }

        @Override
        public void close()
                throws IOException
        {
        }
    }

    private static class CounterTask2
            implements ResumableTask
    {
        private int count;
        private final Phaser phaser;

        public CounterTask2(int count, Phaser phaser)
        {
            this.count = count;
            this.phaser = phaser;
        }

        @Override
        public ControlResult call()
                throws Exception
        {
            if (count == 0) {
                return ControlResult.FINISH;
            }

            count--;
            phaser.arrive();
            return ControlResult.SUSPEND;
        }

        @Override
        public void close()
                throws IOException
        {
        }
    }
}
