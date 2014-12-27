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
package com.facebook.presto.sql.parser;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;

public class OldParser
{
    public static void main(String[] args)
            throws RecognitionException
    {
        String sql = "WITH a AS (SELECT *) VALUES (1),(2)";
        StatementLexer lexer = new StatementLexer(new CaseInsensitiveStream(new ANTLRStringStream(sql)));
        StatementParser parser = new StatementParser(new CommonTokenStream(lexer));

//        CommonTree tree = (CommonTree) parser.expr().getTree();
        CommonTree tree = (CommonTree) parser.statement().getTree();

        System.out.println(tree.toStringTree());
    }
}
