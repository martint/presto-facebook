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

import javax.swing.JDialog;

import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class Main2
{
    public static void main(String[] args)
            throws ExecutionException, InterruptedException, IOException
    {
        String query = "1 IS DISTINCT FROM 2 IS DISTINCT FROM 3";
//        String query = "SELECT * FROM (TABLE a UNION TABLE b)";
//        String query = "WITH a AS (SELECT * FROM orders) VALUES (1),(2)";
//        String query = "SELECT COALESCE(orderkey, custkey), count(*) FROM orders GROUP BY COALESCE(orderkey, custkey)";
//        StatementLexer lexer = new StatementLexer(new ANTLRInputStream(query));
        SqlLexer lexer = new SqlLexer(new CaseInsensitiveStream2(new ANTLRInputStream(query)));
        SqlParser parser = new SqlParser(new CommonTokenStream(lexer));

        ParserRuleContext tree = null; //parser.expression();
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

        latch.await();
    }
}
