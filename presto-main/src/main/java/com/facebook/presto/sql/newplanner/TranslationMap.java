package com.facebook.presto.sql.newplanner;

import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.FieldOrExpression;
import com.facebook.presto.sql.analyzer.TupleDescriptor;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;

import java.util.HashMap;
import java.util.Map;

public class TranslationMap
{
    private final Map<Expression, RelationalExpression> map = new HashMap<>();
    private final Analysis analysis;
    private final TupleDescriptor base;
    private final String tupleVariable;

    public TranslationMap(Analysis analysis, TupleDescriptor base, String tupleVariable)
    {
        this.analysis = analysis;
        this.base = base;
        this.tupleVariable = tupleVariable;
    }

    public void put(Expression expression, RelationalExpression translated)
    {

    }

    public RelationalExpression translate(FieldOrExpression fieldOrExpression)
    {
        if (fieldOrExpression.isFieldReference()) {
            return new FieldRef(new VariableRef(tupleVariable), fieldOrExpression.getFieldIndex());
        }

        return translate(fieldOrExpression.getExpression());
    }

    public RelationalExpression translate(Expression expression)
    {
        return new Translator().process(expression, null);
    }


    private class Translator
        extends AstVisitor<RelationalExpression, Void>
    {
        @Override
        protected RelationalExpression visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        @Override
        protected RelationalExpression visitComparisonExpression(ComparisonExpression node, Void context)
        {
            return new FunctionCall(node.getType().toString(), process(node.getLeft(), context), process(node.getRight(), context));
        }

        @Override
        protected RelationalExpression visitLongLiteral(LongLiteral node, Void context)
        {
            return new ConstantLong(node.getValue());
        }

        @Override
        protected RelationalExpression visitQualifiedNameReference(QualifiedNameReference node, Void context)
        {
            int field = analysis.getResolvedNames(node).get(node.getName());
            return new FieldRef(new VariableRef(tupleVariable), field);
        }

        @Override
        protected RelationalExpression visitArithmeticExpression(ArithmeticExpression node, Void context)
        {
            return new FunctionCall(node.getType().toString(), process(node.getLeft(), context), process(node.getRight(), context));
        }
    }
}
