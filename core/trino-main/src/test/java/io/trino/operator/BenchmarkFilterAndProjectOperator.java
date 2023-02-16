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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSi                        !!!!!!!!                         .*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ExpressionTestUtils.createExpression;
import static io.trino.sql.ExpressionTestUtils.getTypes;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.stream.Collectors.toList;
import static org.openjdk.jmh.annotations.Scope.Thread;

@SuppressWarnings({"PackageVisibleField", "FieldCanBeLocal"})
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 10)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkFilterAndProjectOperator {

    private static final AtomicLong c = new AtomicLong();
    private static final Map<String, Type> TYPE_MAP = ImmutableMap.<String, Type>builder()
            .put("i32", INTEGER)
            .put("i64", BIGINT)
            .put("a", DOUBLE)
            .put("b", DOUBLE)
            .put("constant", DOUBLE)
            .put("d", BOOLEAN)
            .put("e", BOOLEAN)
            .build();
    private static final Metadata METADATA = createTestMetadataManager();
    private static final Session TEST_SESSION = TestingSession.testSessionBuilder().build();
    private static final DataSize FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_SIZE = DataSize.of(500, KILOBYTE);
    private static final int FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_ROW_COUNT = 256;

    private static final LocalMemoryContext memoryContext =  newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());

    @State(Thread)
    public static class Context {
        private final Map<Symbol, Type> symbolTypes = new HashMap<>();
        private final Map<Symbol, Integer> sourceLayout = new HashMap<>();
        private ExecutorService executor;
        private ScheduledExecutorService scheduledExecutor;
        private OperatorFactory operatorFactory;

        @Param({"1000","5000","10000"})
        int batchSize = 1000;

        private List<Page> pages;
        private Page page ;

        private Supplier<PageProcessor> pageProcessor;

        @Param({"i32*i32",
//                "a*b",
//                "a*a",
//                "a*constant",
//                "a*b*b",
//                "a*b*a*a*b*a",
                "i64+i64",
                "a*2.0 + a*3.0 + a * 4.0 + a*5.0",
//                "a=b",
//                "a=constant",
//                "d=e",
//                "a!=b",
//                "a>b",
//                "a<b",
//                "d and e",
//                "d or e",
                "(d OR e) AND (d AND (d != (d OR e)) OR a=b)"
        })
        String expression = "i32*i32";

        @Param({"1000000"})
        int loops = 1000000;

        @Setup
        public void setup() {
            executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
            scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));

            int i = 0;
            TYPE_MAP.forEach((symbolName, type) -> {
                Symbol symbol = new Symbol(symbolName);
                symbolTypes.put(symbol, type);
                sourceLayout.put(symbol, i);
            });

            List<RowExpression> projections = getProjections(expression);
            List<Type> types = projections.stream().map(RowExpression::getType).collect(toList());


            PageFunctionCompiler pageFunctionCompiler = new PageFunctionCompiler(METADATA, 0);
            pageProcessor = new ExpressionCompiler(METADATA, pageFunctionCompiler).compilePageProcessor(Optional.empty(), projections);
            operatorFactory = FilterAndProjectOperator.createOperatorFactory(
                    0,
                    new PlanNodeId("test"),
                    pageProcessor,
                    types,
                    FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_SIZE,
                    FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_ROW_COUNT);

            page = createPage(types, batchSize);
            pages = ImmutableList.of(page);



        }

        private List<RowExpression> getProjections(String expression) {
            ImmutableList.Builder<RowExpression> builder = ImmutableList.builder();
            builder.add(rowExpression(expression));
            return builder.build();
        }

        private RowExpression rowExpression(String value) {
            Expression expression = createExpression(value, METADATA, TypeProvider.copyOf(symbolTypes));

            return SqlToRowExpressionTranslator.translate(
                    expression,
                    getTypes(TEST_SESSION, METADATA, TypeProvider.copyOf(symbolTypes), expression),
                    sourceLayout,
                    METADATA,
                    TEST_SESSION,
                    true);
        }

//        private static List<Page> createInputPages(int positionsPerPage)
//        {
//            ImmutableList.Builder<Page> pages = ImmutableList.builder();
//            PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(BIGINT));
//            LineItemGenerator lineItemGenerator = new LineItemGenerator(1, 1, 1);
//            Iterator<LineItem> iterator = lineItemGenerator.iterator();
//            for (int i = 0; i < TOTAL_POSITIONS; i++) {
//                pageBuilder.declarePosition();
//
//                LineItem lineItem = iterator.next();
//                BIGINT.writeLong(pageBuilder.getBlockBuilder(0), lineItem.getOrderKey());
//
//                if (pageBuilder.getPositionCount() == positionsPerPage) {
//                    pages.add(pageBuilder.build());
//                    pageBuilder.reset();
//                }
//            }
//
//            if (pageBuilder.getPositionCount() > 0) {
//                pages.add(pageBuilder.build());
//            }
//            return pages.build();
//        }

        @TearDown
        public void cleanup() {
            executor.shutdownNow();
            scheduledExecutor.shutdownNow();
        }

        private static Page createPage(List<? extends Type> types, int positions) {

            return SequencePageBuilder.createSequencePage(types, positions);

        }

        public TaskContext createTaskContext() {
            return TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION, DataSize.of(2, GIGABYTE));
        }

        public OperatorFactory getOperatorFactory() {
            return operatorFactory;
        }

        public List<Page> getPages() {
            return pages;
        }

    }

//    @Benchmark
    private long benchmark(Context context) {
        DriverContext driverContext = context.createTaskContext().addPipelineContext(0, true, true, false).addDriverContext();
        OperatorFactory operatorFactory = context.getOperatorFactory();
        Operator operator = operatorFactory.createOperator(driverContext);
//        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();
        long totalSize =0;
        Iterator<Page> input = context.getPages().iterator();
        boolean finishing = false;
        for (int loops = 0; !operator.isFinished() && loops < context.loops; loops++) {
            if (operator.needsInput()) {
                if (input.hasNext()) {
                    Page inputPage = input.next();
                    operator.addInput(inputPage);
                }
                else if (!finishing) {
                    operator.finish();
                    finishing = true;
                }
            }

            Page outputPage = operator.getOutput();
            if (outputPage != null) {
                totalSize += outputPage.getPositionCount();
//                outputPages.add(outputPage);
            }
        }

        return totalSize;
//        return outputPages.build();
    }

    @Benchmark
    @OperationsPerInvocation(1000000)
    public List<Optional<Page>>   pageProcess(Context context) {
//        for (int i = 0; i < context.loops; i++) {
            return ImmutableList.copyOf(context.pageProcessor.get().process(SESSION, new DriverYieldSignal(), memoryContext, context.page));
//        }
//        return null;


    }

    @Test
    public void testBenchmark() {
        Context context = new Context();
        context.setup();
        pageProcess(context);
        context.cleanup();
    }

    public static void main(String[] args)
            throws RunnerException {
        Benchmarks.benchmark(BenchmarkFilterAndProjectOperator.class).run();
    }
}
