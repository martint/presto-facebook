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

import com.facebook.presto.sql.parser2.AstBuilder;
import com.facebook.presto.sql.parser2.CaseInsensitiveStream2;
import com.facebook.presto.sql.parser2.SqlBaseListener;
import com.facebook.presto.sql.parser2.SqlLexer;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.annotations.VisibleForTesting;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.BufferedTreeNodeStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.TreeNodeStream;
import org.antlr.v4.runtime.ANTLRErrorListener;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.ATNConfigSet;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.misc.Pair;

import javax.inject.Inject;

import java.util.BitSet;
import java.util.EnumSet;

import static com.google.common.base.Preconditions.checkNotNull;

public class SqlParser
{
    private final EnumSet<IdentifierSymbol> allowedIdentifierSymbols;

    public SqlParser()
    {
        this(new SqlParserOptions());
    }

    @Inject
    public SqlParser(SqlParserOptions options)
    {
        checkNotNull(options, "options is null");
        allowedIdentifierSymbols = EnumSet.copyOf(options.getAllowedIdentifierSymbols());
    }

    public Statement createStatement(String sql)
    {
        try {
//            return createStatement(parseStatement(sql));
            com.facebook.presto.sql.parser2.SqlParser parser = createNewParser(sql);

            ParserRuleContext tree = parser.singleStatement();

            AstBuilder builder = new AstBuilder();
            Node ast = builder.visit(tree);

            return (Statement) ast;
        }
        catch (StackOverflowError e) {
            throw new ParsingException("statement is too large (stack overflow while parsing)");
        }
    }

    public Expression createExpression(String expression)
    {
        try {
//            return createExpression(parseExpression(expression));
            com.facebook.presto.sql.parser2.SqlParser parser = createNewParser(expression);

            ParserRuleContext tree = parser.singleExpression();

            AstBuilder builder = new AstBuilder();
            Node ast = builder.visit(tree);

            return (Expression) ast;
        }
        catch (StackOverflowError e) {
            throw new ParsingException("expression is too large (stack overflow while parsing)");
        }
    }

    public com.facebook.presto.sql.parser2.SqlParser createNewParser(String sql)
    {
        SqlLexer lexer = new SqlLexer(new CaseInsensitiveStream2(new ANTLRInputStream(sql)));
        com.facebook.presto.sql.parser2.SqlParser parser = new com.facebook.presto.sql.parser2.SqlParser(new org.antlr.v4.runtime.CommonTokenStream(lexer));

        parser.addParseListener(new SqlBaseListener()
        {
            @Override
            public void exitUnquotedIdentifier(@NotNull com.facebook.presto.sql.parser2.SqlParser.UnquotedIdentifierContext ctx)
            {
                String identifier = ctx.IDENTIFIER().getText();
                for (IdentifierSymbol identifierSymbol : EnumSet.complementOf(allowedIdentifierSymbols)) {
                    char symbol = identifierSymbol.getSymbol();
                    if (identifier.indexOf(symbol) >= 0) {
                        throw new ParsingException("identifiers must not contain '" + identifierSymbol.getSymbol() + "'", null, ctx.IDENTIFIER().getSymbol().getLine(), ctx.IDENTIFIER().getSymbol().getCharPositionInLine());
                    }
                }
            }

            @Override
            public void exitBackQuotedIdentifier(@NotNull com.facebook.presto.sql.parser2.SqlParser.BackQuotedIdentifierContext ctx)
            {
                Token token = ctx.BACKQUOTED_IDENTIFIER().getSymbol();
                throw new ParsingException(
                        "backquoted identifiers are not supported; use double quotes to quote identifiers",
                        null,
                        token.getLine(),
                        token.getCharPositionInLine());
            }

            @Override
            public void exitDigitIdentifier(@NotNull com.facebook.presto.sql.parser2.SqlParser.DigitIdentifierContext ctx)
            {
                Token token = ctx.DIGIT_IDENTIFIER().getSymbol();
                throw new ParsingException(
                        "identifiers must not start with a digit; surround the identifier with double quotes",
                        null,
                        token.getLine(),
                        token.getCharPositionInLine());
            }

            @Override
            public void exitQuotedIdentifier(@NotNull com.facebook.presto.sql.parser2.SqlParser.QuotedIdentifierContext ctx)
            {
                // Remove quotes
                // TODO: introduce Identifier class to represent all identifiers in AST
                ctx.getParent().removeLastChild();

                Token token = (Token) ctx.getChild(0).getPayload();
                ctx.getParent().addChild(new CommonToken(
                        new Pair<>(token.getTokenSource(), token.getInputStream()),
                        SqlLexer.IDENTIFIER,
                        token.getChannel(),
                        token.getStartIndex() + 1,
                        token.getStopIndex() - 1));
            }

            @Override
            public void exitNonReserved(@NotNull com.facebook.presto.sql.parser2.SqlParser.NonReservedContext ctx)
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

        lexer.removeErrorListeners();
        parser.removeErrorListeners();

        parser.setErrorHandler(new DefaultErrorStrategy()
        {
            @Override
            protected void reportNoViableAlternative(@NotNull Parser recognizer, @NotNull NoViableAltException e)
            {
                org.antlr.v4.runtime.TokenStream tokens = recognizer.getInputStream();
                String input;
                if (tokens != null) {
                    if (e.getStartToken().getType() == Token.EOF) {
                        input = "<EOF>";
                    }
                    else {
                        // fix ANTLR issue that would result in WS tokens being ignored
                        input = recognizer.getTokenStream().getTokenSource().getInputStream().getText(new Interval(e.getStartToken().getStartIndex(), e.getOffendingToken().getStopIndex()));
                    }
                }
                else {
                    input = "<unknown input>";
                }
                String msg = "no viable alternative at input " + escapeWSAndQuote(input);
                recognizer.notifyErrorListeners(e.getOffendingToken(), msg, e);
            }
        });
        parser.addErrorListener(new ANTLRErrorListener()
        {
            @Override
            public void syntaxError(@NotNull Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, @NotNull String msg, org.antlr.v4.runtime.RecognitionException e)
            {
                throw new ParsingException(msg, e, line, charPositionInLine);
            }

            @Override
            public void reportAmbiguity(@NotNull Parser recognizer, @NotNull DFA dfa, int startIndex, int stopIndex, boolean exact, BitSet ambigAlts, @NotNull ATNConfigSet configs)
            {
//                throw new UnsupportedOperationException("not yet implemented");
            }

            @Override
            public void reportAttemptingFullContext(@NotNull Parser recognizer, @NotNull DFA dfa, int startIndex, int stopIndex, BitSet conflictingAlts, @NotNull ATNConfigSet configs)
            {
//                throw new UnsupportedOperationException("not yet implemented");
            }

            @Override
            public void reportContextSensitivity(@NotNull Parser recognizer, @NotNull DFA dfa, int startIndex, int stopIndex, int prediction, @NotNull ATNConfigSet configs)
            {
//                throw new UnsupportedOperationException("not yet implemented");
            }
        });
        lexer.addErrorListener(new ANTLRErrorListener()
        {
            @Override
            public void syntaxError(@NotNull Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, @NotNull String msg, org.antlr.v4.runtime.RecognitionException e)
            {
                throw new ParsingException(msg, e, line, charPositionInLine);
            }

            @Override
            public void reportAmbiguity(@NotNull Parser recognizer, @NotNull DFA dfa, int startIndex, int stopIndex, boolean exact, BitSet ambigAlts, @NotNull ATNConfigSet configs)
            {
                throw new UnsupportedOperationException("not yet implemented");
            }

            @Override
            public void reportAttemptingFullContext(@NotNull Parser recognizer, @NotNull DFA dfa, int startIndex, int stopIndex, BitSet conflictingAlts, @NotNull ATNConfigSet configs)
            {
                throw new UnsupportedOperationException("not yet implemented");
            }

            @Override
            public void reportContextSensitivity(@NotNull Parser recognizer, @NotNull DFA dfa, int startIndex, int stopIndex, int prediction, @NotNull ATNConfigSet configs)
            {
                throw new UnsupportedOperationException("not yet implemented");
            }
        });

        return parser;
    }

    @VisibleForTesting
    Statement createStatement(CommonTree tree)
    {
        TreeNodeStream stream = new BufferedTreeNodeStream(tree);
        StatementBuilder builder = new StatementBuilder(stream);
        try {
            return builder.statement().value;
        }
        catch (RecognitionException e) {
            throw new AssertionError(e); // RecognitionException is not thrown
        }
    }

    private Expression createExpression(CommonTree tree)
    {
        TreeNodeStream stream = new BufferedTreeNodeStream(tree);
        StatementBuilder builder = new StatementBuilder(stream);
        try {
            return builder.singleExpression().value;
        }
        catch (RecognitionException e) {
            throw new AssertionError(e); // RecognitionException is not thrown
        }
    }

    @VisibleForTesting
    CommonTree parseStatement(String sql)
    {
        try {
            return (CommonTree) getParser(sql).singleStatement().getTree();
        }
        catch (RecognitionException e) {
            throw new AssertionError(e); // RecognitionException is not thrown
        }
    }

    private CommonTree parseExpression(String expression)
    {
        try {
            return (CommonTree) getParser(expression).singleExpression().getTree();
        }
        catch (RecognitionException e) {
            throw new AssertionError(e); // RecognitionException is not thrown
        }
    }

    private StatementParser getParser(String sql)
    {
        CharStream stream = new CaseInsensitiveStream(new ANTLRStringStream(sql));
        StatementLexer lexer = new StatementLexer(stream);
        lexer.setAllowedIdentifierSymbols(allowedIdentifierSymbols);
        TokenStream tokenStream = new CommonTokenStream(lexer);
        return new StatementParser(tokenStream);
    }
}
