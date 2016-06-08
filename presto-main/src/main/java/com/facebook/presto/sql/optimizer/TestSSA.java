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
package com.facebook.presto.sql.optimizer;

import com.facebook.presto.sql.optimizer.engine.SSA;
import com.facebook.presto.sql.optimizer.engine.VariableAllocator;
import com.facebook.presto.sql.optimizer.tree.Filter;
import com.facebook.presto.sql.optimizer.tree.Get;
import com.facebook.presto.sql.optimizer.tree.Project;

public class TestSSA
{
    public static void main(String[] args)
    {
//        System.out.println(
//                new SSA(VariableAllocator.simpleAllocator())
//                        .toSsa(
//                                new Filter(new Project(new Get("t")))
//                        ));
    }
}
