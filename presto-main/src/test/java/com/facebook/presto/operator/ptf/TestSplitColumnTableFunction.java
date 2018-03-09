package com.facebook.presto.operator.ptf;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.TableFunctionOperator.TableFunctionOperatorFactory;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.function.PolymorphicTableFunctionFactory;
import com.facebook.presto.spi.function.TableFunction;
import com.facebook.presto.spi.function.TableFunctionDescriptor;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

@Test(singleThreaded = true)
public class TestSplitColumnTableFunction
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DriverContext driverContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true)
                .addDriverContext();
    }

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testFunction()
    {
        TypeRegistry typeManager = new TypeRegistry();
        new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig());

        PolymorphicTableFunctionFactory factory = new SplitColumnTableFunctionFactory(typeManager);

        TableFunctionDescriptor descriptor = factory.describe(ImmutableMap.<String, Object>builder()
                .put("split_column", utf8Slice("value"))
                .put("delimiter", utf8Slice(" "))
                .put("input", ImmutableList.of(new ColumnMetadata("value", VARCHAR)))
                .put("output", ImmutableList.of(new ColumnMetadata("number", INTEGER), new ColumnMetadata("first", VARCHAR), new ColumnMetadata("second", VARCHAR)))
                .build());

        TableFunction function = factory.getInstance(descriptor.getHandle());
        List<Type> types = descriptor.getOutputColumns().stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());
        OperatorFactory operatorFactory = new TableFunctionOperatorFactory(0, new PlanNodeId("test"), types, function);

        List<Page> input = rowPagesBuilder(VARCHAR)
                .row("1 hello world")
                .row("2 foo bar")
                .pageBreak()
                .row("3 apple store")
                .row("4 orange county")
                .pageBreak()
                .row("5 grape-soda")
                .build();

        List<Page> expected = rowPagesBuilder(VARCHAR, INTEGER, VARCHAR, VARCHAR)
                .row("1 hello world", 1, "hello", "world")
                .row("2 foo bar", 2, "foo", "bar")
                .pageBreak()
                .row("3 apple store", 3, "apple", "store")
                .row("4 orange county", 4, "orange", "county")
                .pageBreak()
                .row("5 grape-soda", 5, "grape-soda", null)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }
}
