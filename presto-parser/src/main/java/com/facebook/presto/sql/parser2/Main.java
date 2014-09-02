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
import org.antlr.v4.runtime.tree.pattern.ParseTreeMatch;
import org.antlr.v4.runtime.tree.pattern.ParseTreePattern;

import javax.swing.JDialog;

import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Main
{
    public static void main(String[] args)
            throws ExecutionException, InterruptedException, IOException
    {
        List<String> queries = Files.readAllLines(Paths.get("/tmp/queries.txt"), UTF_8);

        for (String query : queries) {
            System.out.println(query);

            StatementLexer lexer = new StatementLexer(new CaseInsensitiveStream2(new ANTLRInputStream(query)));
            StatementParser parser = new StatementParser(new CommonTokenStream(lexer));

            ParserRuleContext tree = parser.singleStatement();
//            System.out.println(Trees.toStringTree(tree, parser));
            System.out.println();

//            JDialog dialog = tree.inspect(parser).get();
//            final CountDownLatch latch = new CountDownLatch(1);
//            dialog.addWindowListener(new WindowAdapter()
//            {
//                @Override
//                public void windowClosed(WindowEvent e)
//                {
//                    latch.countDown();
//                }
//            });
//
//            latch.await();
        }

//        AstBuilder builder = new AstBuilder(parser);
//        Node ast = builder.visit(tree);
//        System.out.println(ast);

//        ParseTreePattern pattern = parser.compileParseTreePattern("NOT <booleanExpression>", StatementParser.RULE_expression);
//        ParseTreeMatch match = pattern.match(tree);

//        System.out.println(match.succeeded());
//        ParserRuleContext x = (ParserRuleContext) match.get("booleanExpression");
//        x.inspect(parser).get();

//        if (match.succeeded()) {
//            System.out.println("success");
//        }
//        else {
//            System.out.println("failure");
//        }

        }
    }
