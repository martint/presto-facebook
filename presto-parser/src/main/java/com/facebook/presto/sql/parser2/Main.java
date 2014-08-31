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

import com.facebook.presto.sql.tree.Node;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.Trees;
import org.antlr.v4.runtime.tree.pattern.ParseTreeMatch;
import org.antlr.v4.runtime.tree.pattern.ParseTreePattern;
import org.w3c.dom.traversal.TreeWalker;

import java.util.concurrent.ExecutionException;

public class Main
{
    public static void main(String[] args)
            throws ExecutionException, InterruptedException
    {
//        String sql = "(SELECT 1) UNION (SELECT 2 INTERSECT SELECT 3 ORDER BY 4)";
        String sql = "SELECT A";
        StatementLexer lexer = new StatementLexer(new ANTLRInputStream(sql));
        StatementParser parser = new StatementParser(new CommonTokenStream(lexer));

        ParserRuleContext tree = parser.singleStatement();
        System.out.println(Trees.toStringTree(tree, parser));
        tree.inspect(parser);

//        AstBuilder builder = new AstBuilder(parser);
//        Node ast = builder.visit(tree);
//        System.out.println(ast);

        ParseTreePattern pattern = parser.compileParseTreePattern("NOT <booleanExpression>", StatementParser.RULE_expression);
        ParseTreeMatch match = pattern.match(tree);

        System.out.println(match.succeeded());
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
