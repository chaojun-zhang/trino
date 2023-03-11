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

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.Split;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.UpdatablePageSource;
import io.trino.spi.metrics.Metrics;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.velox.Task;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.lucene.util.IOUtils;

import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.block.RowBlock.fromFieldBlocks;
import static java.util.Objects.requireNonNull;

public class VeloxSourceOperator
        implements SourceOperator {

    @Override
    public PlanNodeId getSourceId() {
        return planNodeId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split) {
        return null;
    }

    @Override
    public void noMoreSplits() {


    }


    public static class VeloxOperatorFactory
            implements SourceOperatorFactory {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private boolean closed;

        public VeloxOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId) {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        }


        @Override
        public PlanNodeId getSourceId() {
            return planNodeId;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext) {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, "VeloxSourceOperator");
            return new VeloxSourceOperator(
                    operatorContext,
                    planNodeId);
        }

        @Override
        public void noMoreOperators() {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate() {
            throw new UnsupportedOperationException("Source operator factories cannot be duplicated");
        }

    }

    private final OperatorContext operatorContext;
    private final PlanNodeId planNodeId;
    private final static BufferAllocator ROOT_ALLOCATOR = new RootAllocator();

    private final LocalMemoryContext systemMemoryContext;

    private final Task veloxTask;


    private boolean finished;

    public VeloxSourceOperator(
            OperatorContext operatorContext,
            PlanNodeId planNodeId) {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.systemMemoryContext = operatorContext.newLocalSystemMemoryContext(VeloxSourceOperator.class.getSimpleName());
        veloxTask = Task.make();
    }

    @Override
    public OperatorContext getOperatorContext() {
        return operatorContext;
    }


    @Override
    public void close() {
        finish();
    }

    @Override
    public void finish() {
        finished = true;
        try {
            veloxTask.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isFinished() {
        return finished || veloxTask.isFinished();
    }

    @Override
    public ListenableFuture<Void> isBlocked() {
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput() {
        return false;
    }

    @Override
    public void addInput(Page page) {
        throw new UnsupportedOperationException(getClass().getName() + " cannot take input");
    }


    private Page getNextPage() {
        Task.ColumnBatch batch = veloxTask.nextBatch(ROOT_ALLOCATOR);
        Block byteArrayBlock = BlockAssertions.createByteArraySequenceBlock(0, 10);
        Block longArrayBlock = BlockAssertions.createLongSequenceBlock(0, 10);
        Block byteArrayBlock1 = BlockAssertions.createByteArraySequenceBlock(0, 10);
        Block longArrayBlock1 = BlockAssertions.createLongSequenceBlock(0, 10);
        Block block = fromFieldBlocks(1, Optional.empty(), new Block[]{longArrayBlock, byteArrayBlock, longArrayBlock1, byteArrayBlock1});
        return new Page(block);
    }

    @Override
    public Page getOutput() {
        Page page = getNextPage();
        // update operator stats
        operatorContext.recordPhysicalInputWithTiming(
                page.getSizeInBytes(),
                page.getPositionCount(),
                0);
        operatorContext.recordProcessedInput(page.getSizeInBytes(), page.getPositionCount());

        // updating system memory usage should happen after page is loaded.
        systemMemoryContext.setBytes(0);
        operatorContext.setLatestMetrics(Metrics.EMPTY);
        finish();
        return page;
    }
}
