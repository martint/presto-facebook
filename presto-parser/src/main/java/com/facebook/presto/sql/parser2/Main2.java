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

import com.facebook.presto.sql.TreePrinter;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Statement;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.misc.Pair;
import org.antlr.v4.runtime.tree.Trees;

import javax.swing.JDialog;

import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class Main2
{
    public static void main(String[] args)
            throws ExecutionException, InterruptedException, IOException
    {
        String query = "WITH a AS (TABLE x) TABLE y";
//        String query = "SELECT * FROM (TABLE a UNION TABLE b)";
//        String query = "WITH a AS (SELECT * FROM orders) VALUES (1),(2)";
//        String query = "SELECT COALESCE(orderkey, custkey), count(*) FROM orders GROUP BY COALESCE(orderkey, custkey)";
//        StatementLexer lexer = new StatementLexer(new ANTLRInputStream(query));
        SqlLexer lexer = new SqlLexer(new CaseInsensitiveStream2(new ANTLRInputStream(query)));
        SqlParser parser = new SqlParser(new CommonTokenStream(lexer));

        Statement old = new com.facebook.presto.sql.parser.SqlParser().createStatement(query);
        new TreePrinter(new IdentityHashMap<>(), System.out).print(old);

        parser.addParseListener(new SqlBaseListener()
        {
            @Override
            public void exitNonReserved(@NotNull SqlParser.NonReservedContext ctx)
            {
                // replace nonReserved words with IDENT tokens

                ctx.getParent().removeLastChild();

                Token token = (Token) ctx.getChild(0).getPayload();
                ctx.getParent().addChild(new CommonToken(
                        new Pair<>(token.getTokenSource(), token.getInputStream()),
                        SqlLexer.IDENTIFIER,
                        token.getChannel(),
                        token.getStartIndex(),
                        token.getStopIndex()));
            }
        });

        ParserRuleContext tree = parser.singleStatement();
        System.out.println(Trees.toStringTree(tree, parser));
        System.out.println();

        JDialog dialog = tree.inspect(parser).get();
        final CountDownLatch latch = new CountDownLatch(1);
        dialog.addWindowListener(new WindowAdapter()
        {
            @Override
            public void windowClosed(WindowEvent e)
            {
                latch.countDown();
            }
        });

        AstBuilder builder = new AstBuilder(parser);
        Node ast = builder.visit(tree);
        new TreePrinter(new IdentityHashMap<>(), System.out).print(ast);


        latch.await();
    }
}
