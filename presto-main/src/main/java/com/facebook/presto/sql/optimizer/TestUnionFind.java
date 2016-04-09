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

import com.google.common.collect.ImmutableSet;
import org.jgrapht.alg.util.UnionFind;

public class TestUnionFind
{
    public static void main(String[] args)
    {
        UnionFind<String> set = new UnionFind<>(ImmutableSet.of());

        set.addElement("g0");
        set.addElement("a");
        set.addElement("b");

        set.union("g0", "a");
        set.union("g0", "b");

        System.out.println(set.find("g0"));
        System.out.println(set.find("a"));
        System.out.println(set.find("b"));
    }
}
