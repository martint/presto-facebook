package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.scalar.UnixTimeFunctions;
import com.facebook.presto.sql.Casts;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Extract;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.InputReference;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NegativeExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.util.ThreadLocalCache;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkNotNull;
import org.joni.Regex;
import org.joni.Option;

public class ExpressionInterpreter
        extends AstVisitor<Object, Void>
{
    private final SymbolResolver symbolResolver;
    private final InputResolver inputResolver;
    private final Metadata metadata;
    private final Session session;
    private final boolean optimize;

    private static final ThreadLocalCache<Slice, Pattern> LIKE_PATTERN_CACHE = new ThreadLocalCache<Slice, Pattern>(20)
    {
        @Override
        protected Pattern load(Slice pattern)
        {
            return likeToPattern(pattern.toString(Charsets.UTF_8), '\\');
        }
    };

    private static final ThreadLocalCache<ByIdentity<LikePredicate>, Regex> CONSTANT_LIKE_PATTERN_CACHE = new ThreadLocalCache<ByIdentity<LikePredicate>, Regex>(20)
    {
        @Override
        protected Regex load(ByIdentity<LikePredicate> pattern)
        {
            return likeToRegex(((StringLiteral) pattern.getObject().getPattern()).getValue(), '\\');
        }
    };

    public static ExpressionInterpreter expressionInterpreter(InputResolver inputResolver, Metadata metadata, Session session)
    {
        checkNotNull(inputResolver, "resolver is null");
        checkNotNull(metadata, "metadata is null");
        checkNotNull(session, "session is null");

        return new ExpressionInterpreter(null, inputResolver, metadata, session, false);
    }

    public static ExpressionInterpreter expressionOptimizer(SymbolResolver symbolResolver, Metadata metadata, Session session)
    {
        checkNotNull(symbolResolver, "resolver is null");
        checkNotNull(metadata, "metadata is null");
        checkNotNull(session, "session is null");

        return new ExpressionInterpreter(symbolResolver, null, metadata, session, true);
    }

    public ExpressionInterpreter(SymbolResolver symbolResolver, InputResolver inputResolver, Metadata metadata, Session session, boolean optimize)
    {
        this.symbolResolver = symbolResolver;
        this.inputResolver = inputResolver;
        this.metadata = metadata;
        this.session = session;
        this.optimize = optimize;
    }

    @Override
    protected Object visitCurrentTime(CurrentTime node, Void context)
    {
        if (node.getType() != CurrentTime.Type.TIMESTAMP) {
            throw new UnsupportedOperationException("not yet implemented: " + node.getType());
        }
        else if (node.getPrecision() != null) {
            throw new UnsupportedOperationException("not yet implemented: non-default precision");
        }

        return UnixTimeFunctions.currentTimestamp(session);
    }

    @Override
    public Object visitInputReference(InputReference node, Void context)
    {
        return inputResolver.getValue(node.getInput());
    }

    @Override
    protected Object visitQualifiedNameReference(QualifiedNameReference node, Void context)
    {
        if (node.getName().getPrefix().isPresent()) {
            // not a symbol
            return node;
        }

        Symbol symbol = Symbol.fromQualifiedName(node.getName());
        return symbolResolver.getValue(symbol);
    }

    @Override
    protected Long visitLongLiteral(LongLiteral node, Void context)
    {
        return node.getValue();
    }

    @Override
    protected Double visitDoubleLiteral(DoubleLiteral node, Void context)
    {
        return node.getValue();
    }

    @Override
    protected Slice visitStringLiteral(StringLiteral node, Void context)
    {
        return node.getSlice();
    }

    @Override
    protected Long visitTimestampLiteral(TimestampLiteral node, Void context)
    {
        return node.getUnixTime();
    }

    @Override
    protected Object visitNullLiteral(NullLiteral node, Void context)
    {
        return null;
    }

    @Override
    protected Object visitIsNullPredicate(IsNullPredicate node, Void context)
    {
        Object value = process(node.getValue(), context);

        if (value instanceof Expression) {
            return node;
        }

        return value == null;
    }

    @Override
    protected Object visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
    {
        Object value = process(node.getValue(), context);

        if (value instanceof Expression) {
            return node;
        }

        return value != null;
    }

    @Override
    protected Object visitSearchedCaseExpression(SearchedCaseExpression node, Void context)
    {
        Expression resultClause = node.getDefaultValue();
        for (WhenClause whenClause : node.getWhenClauses()) {
            Object value = process(whenClause.getOperand(), context);
            if (value instanceof Expression) {
                // TODO: optimize this case
                return node;
            }

            if (Boolean.TRUE.equals(value)) {
                resultClause = whenClause.getResult();
                break;
            }
        }

        if (resultClause == null) {
            return null;
        }

        Object result = process(resultClause, context);
        if (result instanceof Expression) {
            return node;
        }
        return result;
    }

    @Override
    protected Object visitSimpleCaseExpression(SimpleCaseExpression node, Void context)
    {
        Object operand = process(node.getOperand(), context);
        if (operand == null) {
            return null;
        }
        if (operand instanceof Expression) {
            return node;
        }

        Expression resultClause = node.getDefaultValue();
        for (WhenClause whenClause : node.getWhenClauses()) {
            Object value = process(whenClause.getOperand(), context);
            if (value instanceof Expression) {
                // TODO: optimize this case
                return node;
            }

            if (operand instanceof Long && value instanceof  Long) {
                if (((Long) operand).doubleValue() == ((Long) value).doubleValue()) {
                    resultClause = whenClause.getResult();
                    break;
                }
            } else if (operand instanceof Number && value instanceof  Number) {
                if (((Number) operand).doubleValue() == ((Number) value).doubleValue()) {
                    resultClause = whenClause.getResult();
                    break;
                }
            } else if (operand.equals(value)) {
                resultClause = whenClause.getResult();
                break;
            }
        }
        if (resultClause == null) {
            return null;
        }

        Object result = process(resultClause, context);
        if (result instanceof Expression) {
            return node;
        }
        return result;
    }

    @Override
    protected Object visitCoalesceExpression(CoalesceExpression node, Void context)
    {
        for (Expression expression : node.getOperands()) {
            Object value = process(expression, context);

            if (value instanceof Expression) {
                // TODO: optimize this case
                return node;
            }

            if (value != null) {
                return value;
            }
        }
        return null;
    }

    @Override
    protected Object visitInPredicate(InPredicate node, Void context)
    {
        Expression valueListExpression = node.getValueList();
        if (!(valueListExpression instanceof InListExpression)) {
            return node;
        }
        InListExpression valueList = (InListExpression) valueListExpression;

        boolean hasUnresolvedValue = false;
        Object value = process(node.getValue(), context);
        if (value == null) {
            return null;
        }
        if (value instanceof Expression) {
            hasUnresolvedValue = true;
        }

        boolean hasNullValue = false;
        boolean found = false;
        List<Object> values = new ArrayList<>(valueList.getValues().size());
        for (Expression expression : valueList.getValues()) {
            Object inValue = process(expression, context);
            if (value instanceof Expression || inValue instanceof Expression) {
                hasUnresolvedValue = true;
                values.add(inValue);
                continue;
            }

            if (inValue == null) {
                hasNullValue = true;
            }
            else if (!found && value.equals(inValue)) {
                // in does not short-circuit so we must evaluate all value in the list
                found = true;
            }
        }
        if (found) {
            return true;
        }

        if (hasUnresolvedValue) {
            return new InPredicate(toExpression(value), new InListExpression(toExpressions(values)));
        }
        if (hasNullValue) {
            return null;
        }
        return false;
    }

    @Override
    protected Object visitNegativeExpression(NegativeExpression node, Void context)
    {
        Object value = process(node.getValue(), context);
        if (value == null) {
            return null;
        }
        if (value instanceof Expression) {
            return node;
        }

        if (value instanceof Long) {
            return -((long) value);
        }
        return -((double) value);
    }

    @Override
    protected Object visitArithmeticExpression(ArithmeticExpression node, Void context)
    {
        Object left = process(node.getLeft(), context);
        if (left == null) {
            return null;
        }
        Object right = process(node.getRight(), context);
        if (right == null) {
            return null;
        }

        if (left instanceof Expression || right instanceof Expression) {
            return node;
        }

        Number leftNumber = (Number) left;
        Number rightNumber = (Number) right;
        switch (node.getType()) {
            case ADD:
                if (leftNumber instanceof Long && rightNumber instanceof Long) {
                    return leftNumber.longValue() + rightNumber.longValue();
                }
                else {
                    return leftNumber.doubleValue() + rightNumber.doubleValue();
                }
            case SUBTRACT:
                if (leftNumber instanceof Long && rightNumber instanceof Long) {
                    return leftNumber.longValue() - rightNumber.longValue();
                }
                else {
                    return leftNumber.doubleValue() - rightNumber.doubleValue();
                }
            case DIVIDE:
                if (leftNumber instanceof Long && rightNumber instanceof Long) {
                    return leftNumber.longValue() / rightNumber.longValue();
                }
                else {
                    return leftNumber.doubleValue() / rightNumber.doubleValue();
                }
            case MULTIPLY:
                if (leftNumber instanceof Long && rightNumber instanceof Long) {
                    return leftNumber.longValue() * rightNumber.longValue();
                }
                else {
                    return leftNumber.doubleValue() * rightNumber.doubleValue();
                }
            case MODULUS:
                if (leftNumber instanceof Long && rightNumber instanceof Long) {
                    return leftNumber.longValue() % rightNumber.longValue();
                }
                else {
                    return leftNumber.doubleValue() % rightNumber.doubleValue();
                }
            default:
                throw new UnsupportedOperationException("not yet implemented: " + node.getType());
        }
    }

    @Override
    protected Object visitComparisonExpression(ComparisonExpression node, Void context)
    {
        Object left = process(node.getLeft(), context);
        if (left == null) {
            return null;
        }
        Object right = process(node.getRight(), context);
        if (right == null) {
            return null;
        }

        if (left instanceof Number && right instanceof Number) {
            switch (node.getType()) {
                case EQUAL:
                    return ((Number) left).doubleValue() == ((Number) right).doubleValue();
                case NOT_EQUAL:
                    return ((Number) left).doubleValue() != ((Number) right).doubleValue();
                case LESS_THAN:
                    return ((Number) left).doubleValue() < ((Number) right).doubleValue();
                case LESS_THAN_OR_EQUAL:
                    return ((Number) left).doubleValue() <= ((Number) right).doubleValue();
                case GREATER_THAN:
                    return ((Number) left).doubleValue() > ((Number) right).doubleValue();
                case GREATER_THAN_OR_EQUAL:
                    return ((Number) left).doubleValue() >= ((Number) right).doubleValue();
            }
        }
        else if (left instanceof Slice && right instanceof Slice) {
            switch (node.getType()) {
                case EQUAL:
                    return left.equals(right);
                case NOT_EQUAL:
                    return !left.equals(right);
                case LESS_THAN:
                    return ((Slice) left).compareTo((Slice) right) < 0;
                case LESS_THAN_OR_EQUAL:
                    return ((Slice) left).compareTo((Slice) right) <= 0;
                case GREATER_THAN:
                    return ((Slice) left).compareTo((Slice) right) > 0;
                case GREATER_THAN_OR_EQUAL:
                    return ((Slice) left).compareTo((Slice) right) >= 0;
            }
        }

        return new ComparisonExpression(node.getType(), toExpression(left), toExpression(right));
    }

    @Override
    protected Object visitBetweenPredicate(BetweenPredicate node, Void context)
    {
        Object value = process(node.getValue(), context);
        if (value == null) {
            return null;
        }
        Object min = process(node.getMin(), context);
        if (min == null) {
            return null;
        }
        Object max = process(node.getMax(), context);
        if (max == null) {
            return null;
        }

        if (value instanceof Number && min instanceof Number && max instanceof Number) {
            return ((Number) min).doubleValue() <= ((Number) value).doubleValue() && ((Number) value).doubleValue() <= ((Number) max).doubleValue();
        }
        else if (value instanceof Slice && min instanceof Slice && max instanceof Slice) {
            return ((Slice) min).compareTo((Slice) value) <= 0 && ((Slice) value).compareTo((Slice) max) <= 0;
        }

        return new BetweenPredicate(toExpression(value), toExpression(min), toExpression(max));
    }

    @Override
    protected Object visitNullIfExpression(NullIfExpression node, Void context)
    {
        Object first = process(node.getFirst(), context);
        if (first == null) {
            return null;
        }
        Object second = process(node.getSecond(), context);
        if (second == null) {
            return null;
        }

        if (first instanceof Number && second instanceof Number) {
            return ((Number) first).doubleValue() == ((Number) second).doubleValue() ? null : first;
        }
        else if (first instanceof Slice && second instanceof Slice) {
            return first.equals(second) ? null : first;
        }

        return node;
    }

    @Override
    protected Object visitIfExpression(IfExpression node, Void context)
    {
        Object condition = process(node.getCondition(), context);

        if (Boolean.TRUE.equals(condition)) {
            return process(node.getTrueValue(), context);
        }

        if ((condition == null) || (Boolean.FALSE.equals(condition))) {
            if (node.getFalseValue().isPresent()) {
                return process(node.getFalseValue().get(), context);
            }
            return null;
        }

        Object trueValue = process(node.getTrueValue(), context);
        Object falseValue = null;
        if (node.getFalseValue().isPresent()) {
            falseValue = process(node.getFalseValue().get(), context);
        }

        return new IfExpression(toExpression(condition), toExpression(trueValue), toExpression(falseValue));
    }

    @Override
    protected Object visitNotExpression(NotExpression node, Void context)
    {
        Object value = process(node.getValue(), context);
        if (value == null) {
            return null;
        }

        if (value instanceof Expression) {
            return node;
        }

        return !(Boolean) value;
    }

    @Override
    protected Object visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
    {
        Object left = process(node.getLeft(), context);
        Object right = process(node.getRight(), context);

        switch (node.getType()) {
            case AND: {
                // if either left or right is false, result is always false regardless of nulls
                if (Boolean.FALSE.equals(left) || Boolean.TRUE.equals(right)) {
                    return left;
                }

                if (Boolean.FALSE.equals(right) || Boolean.TRUE.equals(left)) {
                    return right;
                }
            }
            case OR: {
                // if either left or right is true, result is always true regardless of nulls
                if (Boolean.TRUE.equals(left) || Boolean.FALSE.equals(right)) {
                    return left;
                }

                if (Boolean.TRUE.equals(right) || Boolean.FALSE.equals(left)) {
                    return right;
                }
            }
        }

        return node;
    }

    @Override
    protected Object visitBooleanLiteral(BooleanLiteral node, Void context)
    {
        return node.equals(BooleanLiteral.TRUE_LITERAL);
    }

    @Override
    protected Object visitFunctionCall(FunctionCall node, Void context)
    {
        // TODO: remove this huge hack
        List<Type> argumentTypes = new ArrayList<>();
        List<Object> argumentValues = new ArrayList<>();
        for (Expression expression : node.getArguments()) {
            Object value = process(expression, context);
            if (value == null) {
                return null;
            }
            Type type;
            if (value instanceof Double) {
                type = Type.DOUBLE;
            }
            else if (value instanceof Long) {
                type = Type.LONG;
            }
            else if (value instanceof Slice) {
                type = Type.STRING;
            }
            else if (value instanceof Boolean) {
                type = Type.BOOLEAN;
            }
            else if (value instanceof Expression) {
                // TODO when we know the type of this expresstion, construct new FunctionCall node with optimized arguments
                return node;
            }
            else {
                throw new RuntimeException("Unhandled value type: " + value.getClass().getName());
            }
            argumentValues.add(value);
            argumentTypes.add(type);
        }
        FunctionInfo function = metadata.getFunction(node.getName(), Lists.transform(argumentTypes, Type.toRaw()));
        // do not optimize non-deterministic functions
        if (optimize && !function.isDeterministic()) {
            return new FunctionCall(node.getName(), node.getWindow().orNull(), node.isDistinct(), toExpressions(argumentValues));
        }
        MethodHandle handle = function.getScalarFunction();
        if (handle.type().parameterCount() > 0 && handle.type().parameterType(0) == Session.class) {
            handle = handle.bindTo(session);
        }
        try {
            return handle.invokeWithArguments(argumentValues);
        }
        catch (Throwable throwable) {
            throw new RuntimeException("Exception from function invocation", throwable);
        }
    }

    @Override
    protected Object visitLikePredicate(LikePredicate node, Void context)
    {
        Object value = process(node.getValue(), context);
        if (!(value instanceof Slice)) {
            return node;
        }

        if (node.getPattern() instanceof StringLiteral) {
            // constant pattern, so look up pattern in identity cache
            Slice slice = (Slice) value;
            Regex regex = CONSTANT_LIKE_PATTERN_CACHE.get(new ByIdentity<>(node));
            org.joni.Matcher matcher = regex.matcher(slice.getBytes());
            int match = matcher.match(0, slice.length(), Option.NONE);
            return match != -1;
        }

        Object pattern = process(node.getPattern(), context);
        if (!(pattern instanceof Slice)) {
            return node;
        }

        String valueString = ((Slice) value).toString(UTF_8);
        Matcher matcher;
        if (node.getEscape() != null) {
            char escapeChar;
            Object escape = process(node.getEscape(), context);
            if (!(escape instanceof Slice)) {
                return node;
            }
            String escapeString = ((Slice) escape).toString(UTF_8);
            if (escapeString.length() == 0) {
                // escaping disabled
                escapeChar = (char) -1; // invalid character
            } else if (escapeString.length() == 1) {
                escapeChar = escapeString.charAt(0);
            } else {
                throw new IllegalArgumentException("escape must be empty or a single character: "  + escapeString);
            }
            String patternString = ((Slice) pattern).toString(UTF_8);
            matcher = likeToPattern(patternString, escapeChar).matcher(valueString);
        }
        else {
            matcher = LIKE_PATTERN_CACHE.get((Slice) pattern).matcher(valueString);
        }

        return matcher.matches();
    }

    public static Regex likeToRegex(String patternString, char escapeChar)
    {
        StringBuilder regex = new StringBuilder(patternString.length() * 2);

        boolean escaped = false;
        for (char currentChar : patternString.toCharArray()) {
            if (currentChar == escapeChar) {
                escaped = true;
            } else {
                switch (currentChar) {
                    case '%':
                        if (escaped) {
                            regex.append("%");
                        }
                        else {
                            regex.append(".*");
                        }
                        escaped = false;
                        break;
                    case '_':
                        if (escaped) {
                            regex.append("_");
                        }
                        else {
                            regex.append('.');
                        }
                        escaped = false;
                        break;
                    default:
                        escaped = false;
                        regex.append(currentChar); //Pattern.quote(String.valueOf(currentChar))); todo
                }
            }
        }
        return new Regex(regex.toString());
    }

    public static Pattern likeToPattern(String patternString, char escapeChar)
    {
        StringBuilder regex = new StringBuilder(patternString.length() * 2);

        boolean escaped = false;
        for (char currentChar : patternString.toCharArray()) {
            if (currentChar == escapeChar) {
                escaped = true;
            } else {
                switch (currentChar) {
                    case '%':
                        if (escaped) {
                            regex.append("%");
                        }
                        else {
                            regex.append(".*");
                        }
                        escaped = false;
                        break;
                    case '_':
                        if (escaped) {
                            regex.append("_");
                        }
                        else {
                            regex.append('.');
                        }
                        escaped = false;
                        break;
                    default:
                        escaped = false;
                        regex.append(Pattern.quote(String.valueOf(currentChar)));
                }
            }
        }
        return Pattern.compile(regex.toString());
    }

    protected Object visitExtract(Extract node, Void context)
    {
        Object value = process(node.getExpression(), context);
        if (value == null) {
            return null;
        }

        if (value instanceof Expression) {
            return new Extract(toExpression(value), node.getField());
        }

        long time = (long) value;
        switch (node.getField()) {
            case CENTURY:
                return UnixTimeFunctions.century(time);
            case YEAR:
                return UnixTimeFunctions.year(time);
            case QUARTER:
                return UnixTimeFunctions.quarter(time);
            case MONTH:
                return UnixTimeFunctions.month(time);
            case WEEK:
                return UnixTimeFunctions.week(time);
            case DAY:
                return UnixTimeFunctions.day(time);
            case DOW:
                return UnixTimeFunctions.dayOfWeek(time);
            case DOY:
                return UnixTimeFunctions.dayOfYear(time);
            case HOUR:
                return UnixTimeFunctions.hour(time);
            case MINUTE:
                return UnixTimeFunctions.minute(time);
            case SECOND:
                return UnixTimeFunctions.second(time);
            case TIMEZONE_HOUR:
            case TIMEZONE_MINUTE:
                return 0L; // we assume all times are UTC for now  TODO
        }

        throw new UnsupportedOperationException("not yet implemented: " + node.getField());
    }

    @Override
    public Object visitCast(Cast node, Void context)
    {
        Object value = process(node.getExpression(), context);

        if (value instanceof Expression) {
            return new Cast((Expression) value, node.getType());
        }

        if (value == null) {
            return null;
        }

        switch (node.getType()) {
            case "BOOLEAN":
                return Casts.toBoolean(value);
            case "VARCHAR":
                return Casts.toSlice(value);
            case "DOUBLE":
                return Casts.toDouble(value);
            case "BIGINT":
                return Casts.toLong(value);
        }

        throw new UnsupportedOperationException("Unsupported type: " + node.getType());
    }

    @Override
    protected Object visitExpression(Expression node, Void context)
    {
        throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
    }

    @Override
    protected Object visitNode(Node node, Void context)
    {
        throw new UnsupportedOperationException("Evaluator visitor can only handle Expression nodes");
    }

    private static List<Expression> toExpressions(List<?> objects)
    {
        return ImmutableList.copyOf(Lists.transform(objects, new Function<Object, Expression>()
        {
            public Expression apply(@Nullable Object value)
            {
                return toExpression(value);
            }
        }));
    }

    public static Expression toExpression(Object object)
    {
        if (object instanceof Expression) {
            return (Expression) object;
        }

        if (object instanceof Long) {
            return new LongLiteral(object.toString());
        }

        if (object instanceof Double) {
            return new DoubleLiteral(object.toString());
        }

        if (object instanceof Slice) {
            return new StringLiteral(((Slice) object).toString(UTF_8));
        }

        if (object instanceof Boolean) {
            return new BooleanLiteral(object.toString());
        }

        if (object == null) {
            return new NullLiteral();
        }

        throw new UnsupportedOperationException("not yet implemented: " + object.getClass().getName());
    }

    private static class ByIdentity<T>
    {
        private final T object;

        private ByIdentity(T object)
        {
            this.object = object;
        }

        private T getObject()
        {
            return object;
        }

        @Override
        public boolean equals(Object o)
        {
            return object == ((ByIdentity<T>) o).object;
        }

        @Override
        public int hashCode()
        {
            return System.identityHashCode(object);
        }
    }


    public static void main(String[] args)
    {
        Regex regex = likeToRegex("%slyly%", '\\');
        Regex regex2 = new Regex(".*slyly.*");

        List<String> values = ImmutableList.of("sdfbawefewr wer", "foo slyly bar");
        for (String value : values) {
            org.joni.Matcher matcher = regex.matcher(value.getBytes());
            int match = matcher.match(0, value.length(), Option.NONE);
            System.out.println(match);
        }

    }
}
