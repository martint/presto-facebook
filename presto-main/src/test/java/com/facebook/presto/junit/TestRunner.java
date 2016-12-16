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
package com.facebook.presto.junit;

import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;

public class TestRunner
    extends Runner
{
    private final Class<?> clazz;

    public TestRunner(Class<?> clazz)
    {
        this.clazz = clazz;
    }

    @Override
    public Description getDescription()
    {
        return TestStateManager.getDescription(clazz);
    }

    @Override
    public void run(RunNotifier notifier)
    {
        TestStateManager.run(clazz, notifier);
    }
}
