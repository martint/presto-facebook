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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import static com.facebook.presto.sql.optimizer.engine.CollectionConstructors.list;

public class ListFormatter
{
    private ListFormatter()
    {
    }

    private static String format(Object list)
    {
        StringWriter out = new StringWriter();
        format(new PrintWriter(out, true), list, 0);
        return out.toString();
    }

    public static int format(PrintWriter out, Object item, int indent)
    {
        if (item instanceof List) {
            return format(out, (List<Object>) item, indent);
        }
        else {
            String value = item.toString();
            out.print(item);
            return value.length();
        }
    }

    private static int format(PrintWriter out, List<Object> list, int continuationIndent)
    {
        out.print("(");
        continuationIndent++;

        if (!list.isEmpty()) {
            int nextIndent = format(out, list.get(0), continuationIndent);

            boolean chop = depth(list.get(0)) > 1;
            if (list.size() > 1) {
                continuationIndent = addSpacing(out, continuationIndent, nextIndent, chop);
                format(out, list.get(1), continuationIndent);
            }

            chop = chop || list.size() > 2 && list.subList(1, list.size()).stream().anyMatch(i -> depth(i) > 1);

            for (int i = 2; i < list.size(); i++) {
                continuationIndent = addSpacing(out, continuationIndent, nextIndent, chop);
                format(out, list.get(i), continuationIndent);
            }
        }
        out.print(")");

        return continuationIndent;
    }

    private static int addSpacing(PrintWriter out, int continuationIndent, int nextIndent, boolean chop)
    {
        if (chop) {
            out.println();
            out.print(indent(continuationIndent));
        }
        else {
            out.print(" ");
            continuationIndent++;
            continuationIndent += nextIndent;
        }
        return continuationIndent;
    }

    private static int depth(Object item)
    {
        if (item instanceof List) {
            return 1 + ((List<?>) item).stream()
                    .map(ListFormatter::depth)
                    .max(Integer::compare)
                    .get();
        }

        return 0;
    }

    private static String indent(int indent)
    {
        return Strings.repeat(" ", indent);
    }

    public static void main(String[] args)
    {
        List<Object> list = list(1, 2, 3, 47,
                list(
                        list(list(4, 5), list(6, 7)),
                        list(list(4, 5), list(6, 7))),
                9);

        System.out.println(format(list));
        System.out.println();

        System.out.println(format(list(1, 2, 3, 4)));
        System.out.println();

        System.out.println(format(list(list(1, 2), list(3, 4), list(5, 6))));
        System.out.println();

        System.out.println(format(list(list(list(1, 2), list(3, 4)), 5, 6)));
        System.out.println();

        System.out.println(format(list(1, 2, 3, 4, 5, list(6, 7), list(8, 9))));
        System.out.println();

        System.out.println(format(list(1, 2, 3, 4, 5, list(list(6, 7), list(8, 9)))));
        System.out.println();

        System.out.println(format(list()));
        System.out.println();

        System.out.println(format(list(1)));
        System.out.println();

        System.out.println(format(list(1, 2)));
        System.out.println();

        System.out.println(format(list(1, 2, 3)));
        System.out.println();

        System.out.println(format(list(1, list(list(2, list(3, 7)), list(4, 5)), 6)));
        System.out.println();

        System.out.println(format(list(
                "if",
                list("and", list(">", "x", "5"), list("<", "y", "3")),
                "foo",
                "bar"
        )));
        System.out.println();
    }
}
