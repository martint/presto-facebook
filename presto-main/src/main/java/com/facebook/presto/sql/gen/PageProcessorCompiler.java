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
package com.facebook.presto.sql.gen;

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.LocalVariableDefinition;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.NamedParameterDefinition;
import com.facebook.presto.byteCode.ParameterizedType;
import com.facebook.presto.byteCode.control.ForLoop;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.operator.PageProcessor;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.Expressions;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Primitives;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.TreeSet;

import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.NamedParameterDefinition.arg;
import static com.facebook.presto.byteCode.OpCodes.NOP;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.control.ForLoop.ForLoopBuilder;
import static com.facebook.presto.byteCode.control.IfStatement.IfStatementBuilder;
import static com.facebook.presto.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static com.facebook.presto.sql.gen.ByteCodeUtils.generateWrite;
import static com.facebook.presto.sql.gen.ByteCodeUtils.invoke;
import static java.lang.String.format;
import static java.util.Collections.nCopies;

public class PageProcessorCompiler
        implements BodyCompiler<PageProcessor>
{
    private final Metadata metadata;

    public PageProcessorCompiler(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public void generateMethods(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, RowExpression filter, List<RowExpression> projections)
    {
        generateProcessMethod(classDefinition, filter, projections);
        generateFilterMethod(classDefinition, callSiteBinder, filter);

        for (int i = 0; i < projections.size(); i++) {
            generateProjectMethod(classDefinition, callSiteBinder, "project_" + i, projections.get(i));
        }
    }

    private void generateProcessMethod(ClassDefinition classDefinition, RowExpression filter, List<RowExpression> projections)
    {
        CompilerContext context = new CompilerContext(BOOTSTRAP_METHOD);
        MethodDefinition method = classDefinition.declareMethod(context,
                a(PUBLIC),
                "process",
                type(int.class),
                arg("session", ConnectorSession.class),
                arg("page", Page.class),
                arg("start", int.class),
                arg("end", int.class),
                arg("pageBuilder", PageBuilder.class));

        LocalVariableDefinition sessionVariable = context.getVariable("session").getLocalVariableDefinition();
        LocalVariableDefinition pageVariable = context.getVariable("page").getLocalVariableDefinition();
        LocalVariableDefinition startVariable = context.getVariable("start").getLocalVariableDefinition();
        LocalVariableDefinition endVariable = context.getVariable("end").getLocalVariableDefinition();
        LocalVariableDefinition pageBuilderVariable = context.getVariable("pageBuilder").getLocalVariableDefinition();

        LocalVariableDefinition positionVariable = context.declareVariable(int.class, "position");

        method.getBody()
                .comment("int position = start;")
                .getVariable(startVariable)
                .putVariable(positionVariable);

        List<Integer> allInputChannels = getInputChannels(Iterables.concat(projections, ImmutableList.of(filter)));
        for (int channel : allInputChannels) {
            LocalVariableDefinition blockVariable = context.declareVariable(com.facebook.presto.spi.block.Block.class, "block_" + channel);
            method.getBody()
                    .comment("Block %s = page.getBlock(%s);", blockVariable.getName(), channel)
                    .getVariable(pageVariable)
                    .push(channel)
                    .invokeVirtual(Page.class, "getBlock", com.facebook.presto.spi.block.Block.class, int.class)
                    .putVariable(blockVariable);
        }

        //
        // for loop loop body
        //
        LabelNode done = new LabelNode("done");

        Block loopBody = new Block(context);

        ForLoopBuilder loop = ForLoop.forLoopBuilder(context)
                .initialize(NOP)
                .condition(new Block(context)
                                .comment("position < end")
                                .getVariable(positionVariable)
                                .getVariable(endVariable)
                                .invokeStatic(CompilerOperations.class, "lessThan", boolean.class, int.class, int.class)
                )
                .update(new Block(context)
                        .comment("position++")
                        .incrementVariable(positionVariable, (byte) 1))
                .body(loopBody);

        loopBody.comment("if (pageBuilder.isFull()) break;")
                .getVariable(pageBuilderVariable)
                .invokeVirtual(PageBuilder.class, "isFull", boolean.class)
                .ifTrueGoto(done);

        // if (filter(cursor))
        IfStatementBuilder filterBlock = new IfStatementBuilder(context);

        Block trueBlock = new Block(context);
        filterBlock.condition(new Block(context).pushThis()
                .getVariable(sessionVariable)
                .append(toGetVariable(context, getInputChannels(filter)))
                .getVariable(positionVariable)
                .invokeVirtual(classDefinition.getType(),
                        "filter",
                        type(boolean.class),
                        ImmutableList.<ParameterizedType>builder()
                                .add(type(ConnectorSession.class))
                                .addAll(nCopies(getInputChannels(filter).size(), type(com.facebook.presto.spi.block.Block.class)))
                                .add(type(int.class))
                                .build()))
                .ifTrue(trueBlock);

        if (projections.size() == 0) {
            trueBlock.getVariable(pageBuilderVariable)
                    .invokeVirtual(PageBuilder.class, "declarePosition", void.class);
        }
        else {
            for (int projectionIndex = 0; projectionIndex < projections.size(); projectionIndex++) {
                List<Integer> inputChannels = getInputChannels(projections.get(projectionIndex));

                trueBlock.pushThis()
                        .getVariable(sessionVariable)
                        .append(toGetVariable(context, inputChannels))
                        .getVariable(positionVariable);

                trueBlock.comment("pageBuilder.getBlockBuilder(" + projectionIndex + ")")
                        .getVariable(pageBuilderVariable)
                        .push(projectionIndex)
                        .invokeVirtual(PageBuilder.class, "getBlockBuilder", BlockBuilder.class, int.class);

                trueBlock.comment("project_" + projectionIndex + "(session, block_" + inputChannels + ", position, blockBuilder)")
                        .invokeVirtual(classDefinition.getType(),
                                "project_" + projectionIndex,
                                type(void.class),
                                ImmutableList.<ParameterizedType>builder()
                                        .add(type(ConnectorSession.class))
                                        .addAll(nCopies(inputChannels.size(), type(com.facebook.presto.spi.block.Block.class)))
                                        .add(type(int.class))
                                        .add(type(BlockBuilder.class))
                                        .build());
            }
        }
        loopBody.append(filterBlock.build());

        method.getBody()
                .append(loop.build())
                .visitLabel(done)
                .comment("return position;")
                .getVariable(positionVariable)
                .retInt();
    }

    private void generateFilterMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, RowExpression filter)
    {
        CompilerContext context = new CompilerContext(BOOTSTRAP_METHOD);
        MethodDefinition method = classDefinition.declareMethod(context,
                a(PUBLIC),
                "filter",
                type(boolean.class),
                ImmutableList.<NamedParameterDefinition>builder()
                        .add(arg("session", ConnectorSession.class))
                        .addAll(toBlockParameters(getInputChannels(filter)))
                        .add(arg("position", int.class))
                        .build());

        method.comment("Filter: %s", filter.toString());

        context.getVariable("position").getLocalVariableDefinition();
        LocalVariableDefinition wasNullVariable = context.declareVariable(type(boolean.class), "wasNull");

        ByteCodeExpressionVisitor visitor = new ByteCodeExpressionVisitor(
                callSiteBinder,
                fieldReferenceCompiler(callSiteBinder),
                metadata.getFunctionRegistry());
        ByteCodeNode body = filter.accept(visitor, context);

        LabelNode end = new LabelNode("end");
        method
                .getBody()
                .comment("boolean wasNull = false;")
                .putVariable(wasNullVariable, false)
                .append(body)
                .getVariable(wasNullVariable)
                .ifFalseGoto(end)
                .pop(boolean.class)
                .push(false)
                .visitLabel(end)
                .retBoolean();
    }

    private void generateProjectMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, String methodName, RowExpression projection)
    {
        CompilerContext context = new CompilerContext(BOOTSTRAP_METHOD);
        MethodDefinition method = classDefinition.declareMethod(context,
                a(PUBLIC),
                methodName,
                type(void.class),
                ImmutableList.<NamedParameterDefinition>builder()
                        .add(arg("session", ConnectorSession.class))
                        .addAll(toBlockParameters(getInputChannels(projection)))
                        .add(arg("position", int.class))
                        .add(arg("output", BlockBuilder.class))
                        .build());

        method.comment("Projection: %s", projection.toString());

        context.getVariable("position").getLocalVariableDefinition();
        LocalVariableDefinition outputVariable = context.getVariable("output").getLocalVariableDefinition();

        LocalVariableDefinition wasNullVariable = context.declareVariable(type(boolean.class), "wasNull");

        Block body = method.getBody()
                .comment("boolean wasNull = false;")
                .putVariable(wasNullVariable, false);

        ByteCodeExpressionVisitor visitor = new ByteCodeExpressionVisitor(callSiteBinder, fieldReferenceCompiler(callSiteBinder), metadata.getFunctionRegistry());

        body.getVariable(outputVariable)
                .comment("evaluate projection: " + projection.toString())
                .append(projection.accept(visitor, context))
                .append(generateWrite(callSiteBinder, context, wasNullVariable, projection.getType()))
                .ret();
    }

    private static List<Integer> getInputChannels(Iterable<RowExpression> expressions)
    {
        TreeSet<Integer> channels = new TreeSet<>();
        for (RowExpression expression : Expressions.subExpressions(expressions)) {
            if (expression instanceof InputReferenceExpression) {
                channels.add(((InputReferenceExpression) expression).getField());
            }
        }
        return ImmutableList.copyOf(channels);
    }

    private static List<Integer> getInputChannels(RowExpression expression)
    {
        return getInputChannels(ImmutableList.of(expression));
    }

    private static List<NamedParameterDefinition> toBlockParameters(List<Integer> inputChannels)
    {
        ImmutableList.Builder<NamedParameterDefinition> parameters = ImmutableList.builder();
        for (int channel : inputChannels) {
            parameters.add(arg("block_" + channel, com.facebook.presto.spi.block.Block.class));
        }
        return parameters.build();
    }

    private static ByteCodeNode toGetVariable(CompilerContext context, List<Integer> inputs)
    {
        Block block = new Block(context);
        for (int channel : inputs) {
            block.getVariable("block_" + channel);
        }
        return block;
    }

    private RowExpressionVisitor<CompilerContext, ByteCodeNode> fieldReferenceCompiler(final CallSiteBinder callSiteBinder)
    {
        return new RowExpressionVisitor<CompilerContext, ByteCodeNode>()
        {
            @Override
            public ByteCodeNode visitInputReference(InputReferenceExpression node, CompilerContext context)
            {
                int field = node.getField();
                Type type = node.getType();

                Class<?> javaType = type.getJavaType();
                Block isNullCheck = new Block(context)
                        .setDescription(format("block_%d.get%s()", field, type))
                        .getVariable("block_" + field)
                        .getVariable("position")
                        .invokeInterface(com.facebook.presto.spi.block.Block.class, "isNull", boolean.class, int.class);

                Block isNull = new Block(context)
                        .putVariable("wasNull", true)
                        .pushJavaDefault(javaType);

                MethodHandle target;
                try {
                    String methodName = "get" + Primitives.wrap(javaType).getSimpleName();
                    target = MethodHandles.lookup().findVirtual(type.getClass(), methodName, MethodType.methodType(javaType, com.facebook.presto.spi.block.Block.class, int.class));
                }
                catch (ReflectiveOperationException e) {
                    throw Throwables.propagate(e);
                }

                Block isNotNull = new Block(context)
                        .getVariable("block_" + field)
                        .getVariable("position")
                        .append(invoke(context, callSiteBinder.bind(target.bindTo(type))));

                return new IfStatement(context, isNullCheck, isNull, isNotNull);
            }

            @Override
            public ByteCodeNode visitCall(CallExpression call, CompilerContext context)
            {
                throw new UnsupportedOperationException("not yet implemented");
            }

            @Override
            public ByteCodeNode visitConstant(ConstantExpression literal, CompilerContext context)
            {
                throw new UnsupportedOperationException("not yet implemented");
            }
        };
    }
}
