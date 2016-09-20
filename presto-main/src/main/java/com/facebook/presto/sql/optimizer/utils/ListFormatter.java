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
package com.facebook.presto.sql.optimizer.utils;

import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.optimizer.utils.CollectionConstructors.list;

public class ListFormatter
{
    private ListFormatter()
    {
    }

    public static String format(Object item)
    {
        return format(item, 0);
    }

    public static String format(Object item, int indent)
    {
        return indent(indent) + formatItem(item, indent).lines.stream().collect(Collectors.joining("\n"));
    }

    private static Block formatItem(Object item, int indent)
    {
        if (!(item instanceof List)) {
            return Block.of(item.toString());
        }

        List<Object> list = (List<Object>) item;

        List<String> result = new ArrayList<>();
        StringBuilder currentLine = new StringBuilder();

        int maxDepth = depth(item) - 1;

        int offset = 0;
        boolean chop = false;
        currentLine.append("(");
        offset++;
        for (int i = 0; i < list.size(); i++) {
            Object element = list.get(i);

            Block block = formatItem(element, indent);

            if (chop) {
                result.add(currentLine.toString());
                currentLine = new StringBuilder(indent(indent + offset));
            }
            else if (i != 0) {
                currentLine.append(" ");
                offset++;
            }

            currentLine.append(block.lines.get(0));
            for (int j = 1; j < block.lines.size(); j++) {
                result.add(currentLine.toString());
                currentLine = new StringBuilder(indent(indent + offset) + block.lines.get(j));
            }

            if (i == 0 && depth(element) > 0 || i >= 1 && maxDepth >= 2) {
                chop = true;
            }

            if (!chop) {
                offset += block.lines.get(0).length();
            }
        }

        currentLine.append(")");
        result.add(currentLine.toString());

        return new Block(result);
    }

    private static int depth(Object item)
    {
        if (item instanceof List) {
            return 1 + ((List<?>) item).stream()
                    .map(ListFormatter::depth)
                    .max(Integer::compare)
                    .orElse(0);
        }

        return 0;
    }

    private static String indent(int indent)
    {
        return Strings.repeat(" ", indent);
    }

    private static class Block
    {
        private final List<String> lines;

        public static Block of(List<String> lines)
        {
            return new Block(lines);
        }

        public static Block of(String value)
        {
            return new Block(list(value));
        }

        private Block(List<String> lines)
        {
            this.lines = lines;
        }
    }

    private static void process(Object object)
    {
        System.out.println(object);
        System.out.println();
        System.out.println(format(object));
        System.out.println();
        System.out.println("--------------------");
    }

    public static void main(String[] args)
    {
        process(list(1, list(list(2, list(3, 7)), list(4, 5)), 6));

        process(list(1, 222, 33, 4444, 5, list(list(6, 7), list(8, 9))));
        process(list(list(1, 2), list(3, 4), list(5, 6)));
        process(list(list(list(1, 2), list(3, 4)), 5, 6));

        process(
                list("lambda",
                        list("r"),
                        list("let",
                                list(
                                        list("v1", 1),
                                        list("v2", list("add", list("v1", 2))),
                                        list("v3", list("sub", list("v2", "r")))),
                                "v3")));

        process(
                list(1, 2, 3, 47,
                        list(
                                list(list(4, 5), list(6, 7)),
                                list(list(4, 5), list(6, 7))),
                        9));

        process(list(1, 2, 3, 4));

        process(list(1, 2, 3, 4, 5, list(6, 7), list(8, 9)));

        process(list());
        process(list(1));
        process(list(1, 2));
        process(list(1, 2, 3));

        process(list(
                "if",
                list("and", list(">", "x", "5"), list("<", "y", "3")),
                "foo",
                "bar"
        ));
    }
}
