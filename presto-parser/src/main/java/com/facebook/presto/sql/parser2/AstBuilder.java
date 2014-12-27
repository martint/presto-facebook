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

public class AstBuilder
//        extends BaseStatementVisitor<Node>
{
    private final SqlParser parser;

    public AstBuilder(SqlParser parser)
    {
        this.parser = parser;
    }
//
//    public Node visitExpression(@NotNull SqlParser.ExpressionContext ctx)
//    {
//        Optional<Map<String, ParseTree>> match = match(ctx, pattern(token(SqlParser.NOT), rule("child", SqlParser.RULE_booleanExpression)));
//        if (match.isPresent()) {
//            return new NotExpression((Expression) process(match.get(), "child"));
//        }
//
//        return null;
//    }
//
//    private Node process(Map<String, ParseTree> match, String label)
//    {
//        return visit(match.get(label));
//    }
//
//    public static Optional<Map<String, ParseTree>> match(ParseTree node, Object pattern)
//    {
//        Map<String, ParseTree> result = new HashMap<>();
//        return Optional.of(result);
//    }
//
//    public static Object pattern(Object... items)
//    {
//        return items;
//    }
//
//    public static Object token(int tokenId)
//    {
//        return "token:" + tokenId;
//    }
//
//    public static Object rule(String name, int ruleId)
//    {
//        return "rule:" + ruleId + "[" + name + "]";
//    }
}
