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

import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(TestRunner.class)
public class B
{
    public SuiteScopedState suiteScopedState;
    public ClassScopedState classScopedField;
    public TestScopedState testScopedField;

    @Test
    public void test1(SuiteScopedState suiteScoped, ClassScopedState classScoped, TestScopedState testScoped)
    {
        System.err.println("b.test1");
    }

    @Test
    public void test2(SuiteScopedState suiteScoped, ClassScopedState classScoped, TestScopedState testScoped)
    {
        System.err.println("b.test2");
    }
}
