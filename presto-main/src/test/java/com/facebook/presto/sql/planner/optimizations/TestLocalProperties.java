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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.spi.ConstantProperty;
import com.facebook.presto.spi.GroupingProperty;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.SortingProperty;
import com.facebook.presto.spi.block.SortOrder;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class TestLocalProperties
{
    @SafeVarargs
    private static <T> void assertMatch(List<LocalProperty<T>> actual, List<LocalProperty<T>> wanted, Optional<LocalProperty<T>>... match)
    {
        assertEquals(LocalProperty.match(actual, wanted), Arrays.asList(match));
    }

    @Test
    public void test1()
            throws Exception
    {
        List<LocalProperty<String>> actual = builder()
                .grouped("a")
                .grouped("b")
                .grouped("c")
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c", "d").build(),
                Optional.of(grouped("d")));

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("b").build(),
                Optional.of(grouped("b")));

        assertMatch(
                actual,
                builder().grouped("b", "c").build(),
                Optional.of(grouped("b", "c")));

        assertMatch(
                actual,
                builder().grouped("a", "c").build(),
                Optional.of(grouped("c")));

        assertMatch(
                actual,
                builder().grouped("c").build(),
                Optional.of(grouped("c")));
    }

    @Test
    public void test2()
            throws Exception
    {
        List<LocalProperty<String>> actual = builder()
                .grouped("a", "b", "c")
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c", "d").build(),
                Optional.of(grouped("d")));

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.of(grouped("a", "b")));

        assertMatch(
                actual,
                builder().grouped("a").build(),
                Optional.of(grouped("a")));
    }

    @Test
    public void test3()
            throws Exception
    {
        List<LocalProperty<String>> actual = builder()
                .grouped("a", "b")
                .grouped("c")
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c", "d").build(),
                Optional.of(grouped("d")));

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "c").build(),
                Optional.of(grouped("a", "c")));

        assertMatch(
                actual,
                builder().grouped("a").build(),
                Optional.of(grouped("a")));

        assertMatch(
                actual,
                builder().grouped("c").build(),
                Optional.of(grouped("c")));
    }

    @Test
    public void test4()
            throws Exception
    {
        List<LocalProperty<String>> actual = builder()
                .grouped("a", "b")
                .grouped("c")
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c", "d").build(),
                Optional.of(grouped("d")));

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("b", "c").build(),
                Optional.of(grouped("b", "c")));

        assertMatch(
                actual,
                builder().grouped("a").build(),
                Optional.of(grouped("a")));

        assertMatch(
                actual,
                builder().grouped("c").build(),
                Optional.of(grouped("c")));
    }

    @Test
    public void test5()
            throws Exception
    {
        List<LocalProperty<String>> actual = builder()
                .sorted("a", SortOrder.ASC_NULLS_FIRST)
                .sorted("b", SortOrder.ASC_NULLS_FIRST)
                .sorted("c", SortOrder.ASC_NULLS_FIRST)
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c", "d").build(),
                Optional.of(grouped("d")));

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("b", "c").build(),
                Optional.of(grouped("b", "c")));

        assertMatch(
                actual,
                builder().grouped("b").build(),
                Optional.of(grouped("b")));
    }

    @Test
    public void test6()
            throws Exception
    {
        List<LocalProperty<String>> actual = builder()
                .sorted("a", SortOrder.ASC_NULLS_FIRST)
                .grouped("b", "c")
                .sorted("d", SortOrder.ASC_NULLS_FIRST)
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c", "d", "e").build(),
                Optional.of(grouped("e")));

        assertMatch(
                actual,
                builder().grouped("a", "b", "c", "d").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.of(grouped("b")));

        assertMatch(
                actual,
                builder().grouped("a").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("b").build(),
                Optional.of(grouped("b")));

        assertMatch(
                actual,
                builder().grouped("d").build(),
                Optional.of(grouped("d")));

    }

    @Test
    public void test7()
            throws Exception
    {
        List<LocalProperty<String>> actual = builder()
                .constant("a")
                .grouped("a", "b")
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.of(grouped("c")));

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("b").build(),
                Optional.empty());
    }

    @Test
    public void test9()
            throws Exception
    {
        List<LocalProperty<String>> actual = builder()
                .constant("a")
                .grouped("b")
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.of(grouped("c")));

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("b").build(),
                Optional.empty());

    }

    @Test
    public void test10()
            throws Exception
    {
        List<LocalProperty<String>> actual = builder()
                .constant("a")
                .grouped("a", "b")
                .grouped("a", "c")
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c", "d").build(),
                Optional.of(grouped("d")));

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "c").build(),
                Optional.of(grouped("c")));

        assertMatch(
                actual,
                builder().grouped("b").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("b", "c").build(),
                Optional.empty());
    }

    @Test
    public void test11()
            throws Exception
    {
        List<LocalProperty<String>> actual = builder()
                .constant("b")
                .sorted("a", SortOrder.ASC_NULLS_FIRST)
                .sorted("b", SortOrder.ASC_NULLS_FIRST)
                .sorted("c", SortOrder.ASC_NULLS_FIRST)
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "d").build(),
                Optional.of(grouped("d")));

        assertMatch(
                actual,
                builder().grouped("a", "c").build(),
                Optional.empty());
    }

    private static GroupingProperty<String> grouped(String... columns)
    {
        return new GroupingProperty<>(Arrays.asList(columns));
    }

    private static Builder builder()
    {
        return new Builder();
    }

    private static class Builder
    {
        private final List<LocalProperty<String>> properties = new ArrayList<>();

        public Builder grouped(String... columns)
        {
            properties.add(new GroupingProperty<>(Arrays.asList(columns)));
            return this;
        }

        public Builder sorted(String column, SortOrder order)
        {
            properties.add(new SortingProperty<>(column, order));
            return this;
        }

        public Builder constant(String column)
        {
            properties.add(new ConstantProperty<>(column));
            return this;
        }

        public List<LocalProperty<String>> build()
        {
            return new ArrayList<>(properties);
        }
    }
}
