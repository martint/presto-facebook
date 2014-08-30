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

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.Trees;

import java.util.concurrent.ExecutionException;

public class Main
{
    public static void main(String[] args)
            throws ExecutionException, InterruptedException
    {
        String sql = "1 BETWEEN NOT TRUE AND FALSE";
        StatementLexer lexer = new StatementLexer(new ANTLRInputStream(sql));
        StatementParser parser = new StatementParser(new CommonTokenStream(lexer));

        ParserRuleContext result = parser.expr();
        System.out.println(Trees.toStringTree(result, parser));
        result.inspect(parser).get();
    }
}
