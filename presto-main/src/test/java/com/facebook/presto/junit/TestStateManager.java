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

import com.google.common.base.Throwables;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunNotifier;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class TestStateManager
{
    private static final AtomicBoolean finished = new AtomicBoolean();
    private static final ConcurrentMap<Class<?>, Object> suiteStates = new ConcurrentHashMap<>();

    public static Description getDescription(Class<?> clazz)
    {
        return Description.createSuiteDescription(clazz);
    }

    public static void run(Class<?> clazz, RunNotifier notifier)
    {
        addListener(notifier);

        try {
            // 1. for each test method
            //   2. create a new instance of the class
            //   3. inject all public fields with state objects
            //     4. if class-scoped, lookup in cache or instantiate and store
            //     5. if test-scoped, instantiate
            //   6. repeat for test method args
            //   7. invoke method
            //   8. tear-down test-scoped states
            // 9. tear-down class-scoped states

            List<Field> fields = new ArrayList<>();
            for (Field field : clazz.getFields()) {
                TestState testState = field.getType().getAnnotation(TestState.class);
                if (testState != null) {
                    fields.add(field);
                }
            }

            Map<Class<?>, Object> classStates = new HashMap<>();
            try {
                for (Method method : clazz.getMethods()) {
                    if (method.getAnnotation(Test.class) != null) {
                        Map<Class<?>, Object> testStates = new HashMap<>();
                        try {
                            Object instance = clazz.newInstance();
                            for (Field field : fields) {
                                Class<?> type = field.getType();
                                Object state = getStateObject(classStates, testStates, type);
                                field.set(instance, state);
                            }

                            Object[] arguments = new Object[method.getParameterCount()];
                            for (int i = 0; i < method.getParameterTypes().length; i++) {
                                arguments[i] = getStateObject(classStates, testStates, method.getParameterTypes()[i]);
                            }

                            method.invoke(instance, arguments);
                        }
                        finally {
                            for (Object state : testStates.values()) {
                                teardown(state);
                            }
                        }
                    }
                }
            }
            finally {
                for (Object state : classStates.values()) {
                    teardown(state);
                }
            }
        }
        catch (Exception e)
        {
            notifier.fireTestFailure(new Failure(Description.createSuiteDescription("suite"), e));
        }
    }

    private static Object instantiate(Class<?> clazz)
    {
        try {
            Object state = clazz.newInstance();

            Stream.of(state.getClass().getMethods())
                    .filter(method -> method.getAnnotation(Setup.class) != null)
                    .forEach(method ->{
                        try {
                            method.invoke(state);
                        }
                        catch (Exception e) {
                            throw Throwables.propagate(e);
                        }
                    });

            return state;
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
    private static void teardown(Object state)
    {
        Stream.of(state.getClass().getMethods())
                .filter(method -> method.getAnnotation(TearDown.class) != null)
                .forEach(method ->{
                    try {
                        method.invoke(state);
                    }
                    catch (Exception e) {
                        throw Throwables.propagate(e);
                    }
                });
    }

    private static Object getStateObject(Map<Class<?>, Object> classStates, Map<Class<?>, Object> testStates, Class<?> type)
    {
        Scope scope = type.getAnnotation(TestState.class).value();

        Object state = null;
        switch (scope) {
            case SUITE:
                state = suiteStates.computeIfAbsent(type, TestStateManager::instantiate);
                break;
            case CLASS:
                state = classStates.computeIfAbsent(type, TestStateManager::instantiate);
                break;
            case TEST:
                state = testStates.computeIfAbsent(type, TestStateManager::instantiate);
                break;
        }
        return state;
    }

    private static void shutdown()
    {
        if (!finished.getAndSet(true)) {
            suiteStates.values()
                    .forEach(TestStateManager::teardown);
        }
    }

    private static void addListener(RunNotifier notifier)
    {
        notifier.addListener(new RunListener() {
            @Override
            public void testRunFinished(Result result)
                    throws Exception
            {
                shutdown();
            }
        });
    }
}
