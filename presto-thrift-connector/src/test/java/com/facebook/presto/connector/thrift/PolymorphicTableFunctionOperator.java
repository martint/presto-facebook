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
package com.facebook.presto.connector.thrift;

import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.TopNOperator;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;

import java.util.List;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static java.util.Objects.requireNonNull;

public class PolymorphicTableFunctionOperator
        implements Operator
{
    public static class PolymorphicTableFunctionOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> types;
        private final Function<ConnectorPageSource, ConnectorPageSource> polymorphicTableFunctionSupplier;
        private boolean closed;

        public PolymorphicTableFunctionOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> types,
                Function<ConnectorPageSource, ConnectorPageSource> polymorphicTableFunctionSupplier)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.polymorphicTableFunctionSupplier = requireNonNull(polymorphicTableFunctionSupplier, "polymorphicTableFunctionSupplier is null");
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, TopNOperator.class.getSimpleName());
            return new PolymorphicTableFunctionOperator(
                    operatorContext,
                    types,
                    polymorphicTableFunctionSupplier);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new PolymorphicTableFunctionOperatorFactory(operatorId, planNodeId, types, polymorphicTableFunctionSupplier);
        }
    }

    private final OperatorContext operatorContext;
    private final List<Type> types;
    private final Function<ConnectorPageSource, ConnectorPageSource> polymorphicTableFunctionSupplier;
    private final InputPageSource inputPageSource = new InputPageSource();
    @Nullable
    private ConnectorPageSource outputPageSource;

    public PolymorphicTableFunctionOperator(
            OperatorContext operatorContext,
            List<Type> types,
            Function<ConnectorPageSource, ConnectorPageSource> polymorphicTableFunctionSupplier)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.polymorphicTableFunctionSupplier = requireNonNull(polymorphicTableFunctionSupplier, "polymorphicTableFunctionSupplier is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (!inputPageSource.hasPendingPage()) {
            return NOT_BLOCKED;
        }
        if (outputPageSource == null) {
            return NOT_BLOCKED;
        }
        return toListenableFuture(outputPageSource.isBlocked());
    }

    @Override
    public boolean needsInput()
    {
        return !inputPageSource.hasPendingPage() && !inputPageSource.isFinished();
    }

    @Override
    public void addInput(Page page)
    {
        inputPageSource.setPendingPage(page);
        if (outputPageSource == null) {
            outputPageSource = polymorphicTableFunctionSupplier.apply(inputPageSource);
        }
    }

    @Override
    public Page getOutput()
    {
        if (outputPageSource == null)  {
            return null;
        }
        Page page = outputPageSource.getNextPage();
        return page;
    }

    @Override
    public void finish()
    {
        inputPageSource.close();
    }

    @Override
    public boolean isFinished()
    {
        if (outputPageSource == null)  {
            return inputPageSource.isFinished();
        }
        return outputPageSource.isFinished();
    }

    @Override
    public void close()
            throws Exception
    {
        try {
            inputPageSource.close();
        }
        finally {
            if (outputPageSource != null) {
                outputPageSource.close();
            }
        }
    }

    private static class InputPageSource
            implements ConnectorPageSource
    {
        private Page pendingPage;
        private boolean finished;

        public void setPendingPage(Page pendingPage)
        {
            requireNonNull(pendingPage, "pendingPage is null");
            verify(this.pendingPage == null);
            this.pendingPage = pendingPage;
        }

        public boolean hasPendingPage()
        {
            return pendingPage != null;
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public boolean isFinished()
        {
            return finished;
        }

        @Override
        public Page getNextPage()
        {
            Page page = pendingPage;
            pendingPage = null;
            return page;
        }

        @Override
        public long getSystemMemoryUsage()
        {
            return pendingPage == null ? 0 : pendingPage.getRetainedSizeInBytes();
        }

        @Override
        public void close()
        {
            pendingPage = null;
            finished = true;
        }
    }
}
