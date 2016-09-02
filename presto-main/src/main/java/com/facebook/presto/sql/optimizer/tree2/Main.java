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
package com.facebook.presto.sql.optimizer.tree2;

import com.facebook.presto.sql.optimizer.utils.ListFormatter;

import static com.facebook.presto.sql.optimizer.utils.CollectionConstructors.list;

public class Main
{
    public static void main(String[] args)
    {
        Expression expression =
                new Lambda("r",
                        new Let(
                                list(
                                        new Assignment("v1", new Value(1)),
                                        new Assignment("v2", new Call("add", list(new Reference("v1"), new Value(2)))),
                                        new Assignment("v3", new Call("sub", list(new Reference("v2"), new Value("r"))))),
                                new Reference("v2")));

        System.out.println(ListFormatter.format(expression.terms()));
    }
}
