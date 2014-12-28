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

import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NegativeExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.collect.ImmutableList;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.TerminalNode;

public class AstBuilder
        extends SqlBaseVisitor<Node>
{
    private final SqlParser parser;

    public AstBuilder(SqlParser parser)
    {
        this.parser = parser;
    }

    // boolean expressions

    @Override
    public Node visitLogicalNot(@NotNull SqlParser.LogicalNotContext ctx)
    {
        return new NotExpression((Expression) visit(ctx.booleanExpression()));
    }

    @Override
    public Node visitLogicalBinary(@NotNull SqlParser.LogicalBinaryContext ctx)
    {
        LogicalBinaryExpression.Type type;

        switch (ctx.operator.getType()) {
            case SqlLexer.AND:
                type = LogicalBinaryExpression.Type.AND;
                break;
            case SqlLexer.OR:
                type = LogicalBinaryExpression.Type.OR;
                break;
            default:
                throw new IllegalArgumentException("Unsupported operator: " + ctx.operator.getText());
        }

        return new LogicalBinaryExpression(
                type,
                (Expression) visit(ctx.left),
                (Expression) visit(ctx.right));
    }

    // predicates

    @Override
    public Node visitComparison(@NotNull SqlParser.ComparisonContext ctx)
    {
        ComparisonExpression.Type type;

        TerminalNode operator = (TerminalNode) ctx.comparisonOperator().getChild(0);
        switch (operator.getSymbol().getType()) {
            case SqlLexer.EQ:
                type = ComparisonExpression.Type.EQUAL;
                break;
            case SqlLexer.NEQ:
                type = ComparisonExpression.Type.NOT_EQUAL;
                break;
            case SqlLexer.LT:
                type = ComparisonExpression.Type.LESS_THAN;
                break;
            case SqlLexer.LTE:
                type = ComparisonExpression.Type.LESS_THAN_OR_EQUAL;
                break;
            case SqlLexer.GT:
                type = ComparisonExpression.Type.GREATER_THAN;
                break;
            case SqlLexer.GTE:
                type = ComparisonExpression.Type.GREATER_THAN_OR_EQUAL;
                break;
            default:
                throw new IllegalArgumentException("Unsupported operator: " + operator.getSymbol().getText());
        }

        return new ComparisonExpression(type, (Expression) visit(ctx.left), (Expression) visit(ctx.right));
    }

    @Override
    public Node visitDistinctFrom(@NotNull SqlParser.DistinctFromContext ctx)
    {
        Expression expression = new ComparisonExpression(
                ComparisonExpression.Type.IS_DISTINCT_FROM,
                (Expression) visit(ctx.left),
                (Expression) visit(ctx.right));

        if (ctx.NOT() != null) {
            expression = new NotExpression(expression);
        }

        return expression;
    }

    @Override
    public Node visitBetween(@NotNull SqlParser.BetweenContext ctx)
    {
        return new BetweenPredicate(
                (Expression) visit(ctx.value),
                (Expression) visit(ctx.lower),
                (Expression) visit(ctx.upper));
    }

    @Override
    public Node visitNullPredicate(@NotNull SqlParser.NullPredicateContext ctx)
    {
        Expression child = (Expression) visit(ctx.valueExpression());

        if (ctx.NOT() == null) {
            return new IsNullPredicate(child);
        }

        return new IsNotNullPredicate(child);
    }

    @Override
    public Node visitLike(@NotNull SqlParser.LikeContext ctx)
    {
        Expression escape = null;
        if (ctx.escape != null) {
            escape = (Expression) visit(ctx.escape);
        }

        Expression result = new LikePredicate((Expression) visit(ctx.value), (Expression) visit(ctx.pattern), escape);

        if (ctx.NOT() != null) {
            result = new NotExpression(result);
        }

        return result;
    }

    // ************** value expressions **************

    @Override
    public Node visitArithmeticNegation(@NotNull SqlParser.ArithmeticNegationContext ctx)
    {
        return new NegativeExpression((Expression) visit(ctx.valueExpression()));
    }

    @Override
    public Node visitArithmeticBinary(@NotNull SqlParser.ArithmeticBinaryContext ctx)
    {
        ArithmeticExpression.Type type;

        switch (ctx.operator.getType()) {
            case SqlLexer.PLUS:
                type = ArithmeticExpression.Type.ADD;
                break;
            case SqlLexer.MINUS:
                type = ArithmeticExpression.Type.SUBTRACT;
                break;
            case SqlLexer.ASTERISK:
                type = ArithmeticExpression.Type.MULTIPLY;
                break;
            case SqlLexer.SLASH:
                type = ArithmeticExpression.Type.DIVIDE;
                break;
            case SqlLexer.PERCENT:
                type = ArithmeticExpression.Type.MODULUS;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operator: " + ctx.operator.getText());
        }

        return new ArithmeticExpression(
                type,
                (Expression) visit(ctx.left),
                (Expression) visit(ctx.right));
    }

    @Override
    public Node visitConcatenation(@NotNull SqlParser.ConcatenationContext ctx)
    {
        return new FunctionCall(new QualifiedName("concat"), ImmutableList.of(
                (Expression) visit(ctx.left),
                (Expression) visit(ctx.right)));
    }

    // primary expression

    @Override
    public Node visitSpecialDateTimeFunction(@NotNull SqlParser.SpecialDateTimeFunctionContext ctx)
    {
        CurrentTime.Type type;

        switch (ctx.name.getType()) {
            case SqlLexer.CURRENT_DATE:
                type = CurrentTime.Type.DATE;
                break;
            case SqlLexer.CURRENT_TIME:
                type = CurrentTime.Type.TIME;
                break;
            case SqlLexer.CURRENT_TIMESTAMP:
                type = CurrentTime.Type.TIMESTAMP;
                break;
            case SqlLexer.LOCALTIME:
                type = CurrentTime.Type.LOCALTIME;
                break;
            case SqlLexer.LOCALTIMESTAMP:
                type = CurrentTime.Type.LOCALTIMESTAMP;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported special function: " + ctx.name.getText());
        }

        if (ctx.precision != null) {
            return new CurrentTime(type, Integer.parseInt(ctx.precision.getText()));
        }

        return new CurrentTime(type);
    }

    // ************** literals **************

    @Override
    public Node visitNullLiteral(@NotNull SqlParser.NullLiteralContext ctx)
    {
        return new NullLiteral();
    }

    @Override
    public Node visitStringLiteral(@NotNull SqlParser.StringLiteralContext ctx)
    {
        return new StringLiteral(unquote(ctx.STRING().getText()));
    }

    @Override
    public Node visitTypeConstructor(@NotNull SqlParser.TypeConstructorContext ctx)
    {
        return new GenericLiteral(ctx.identifier().getText(), unquote(ctx.STRING().getText()));
    }

    @Override
    public Node visitIntegerLiteral(@NotNull SqlParser.IntegerLiteralContext ctx)
    {
        return new LongLiteral(ctx.getText());
    }

    @Override
    public Node visitDecimalLiteral(@NotNull SqlParser.DecimalLiteralContext ctx)
    {
        return new DoubleLiteral(ctx.getText());
    }

    @Override
    public Node visitBooleanValue(@NotNull SqlParser.BooleanValueContext ctx)
    {
        return new BooleanLiteral(ctx.getText());
    }

    @Override
    protected Node defaultResult()
    {
        return null;
    }

    @Override
    protected Node aggregateResult(Node aggregate, Node nextResult)
    {
        if (nextResult == null) {
            throw new UnsupportedOperationException("not yet implemented");
        }

        if (aggregate == null) {
            return nextResult;
        }

        throw new UnsupportedOperationException("not yet implemented");
    }

    private static String unquote(String string)
    {
        return string.substring(1, string.length() - 1);
    }
}
