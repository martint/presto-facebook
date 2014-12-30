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
package com.facebook.presto.sql.parser2;

import com.google.common.base.Joiner;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;

import java.util.Collections;

public class Main3
{
    private Main3() {}
    public static void main(String[] args)
    {
        int terms = 1000;

        for (int i = 0; i < 1000; i++) {
            terms++;
//            terms = ThreadLocalRandom.current().nextInt(1, 1000);
            String expression =  Joiner.on("+").join(Collections.nCopies(terms, 1));

            double time = parse(expression);
            System.out.printf("%d: %.2f ms, %.2f µs/term\n", terms, time, time * 1000 / terms);
        }
    }

    private static double parse(String expression)
    {
        TestLexer lexer = new TestLexer(new CaseInsensitiveStream2(new ANTLRInputStream(expression)));
        TestParser parser = new TestParser(new CommonTokenStream(lexer));

        long start = System.nanoTime();
        parser.expression();
        return (System.nanoTime() - start) / 1e6;
    }
}
