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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.Split;
import io.trino.spi.Page;
import io.trino.spi.metrics.Metrics;
import io.trino.split.PageSourceProvider;
import io.trino.sql.planner.plan.PlanNodeId;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class VeloxSourceOperator
        implements Operator {

    public static class VeloxOperatorFactory
            implements OperatorFactory {
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
        public Operator createOperator(DriverContext driverContext) {
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

    private final LocalMemoryContext systemMemoryContext;
    private final SettableFuture<Void> blocked = SettableFuture.create();

    @Nullable
    private Split split;

    private boolean finished;

    public VeloxSourceOperator(
            OperatorContext operatorContext,
            PlanNodeId planNodeId) {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.systemMemoryContext = operatorContext.newLocalSystemMemoryContext(VeloxSourceOperator.class.getSimpleName());
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
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public ListenableFuture<Void> isBlocked() {
        return NOT_BLOCKED;
    }

    private static <T> ListenableFuture<Void> asVoid(ListenableFuture<T> future) {
        return Futures.transform(future, v -> null, directExecutor());
    }

    @Override
    public boolean needsInput() {
        return false;
    }

    @Override
    public void addInput(Page page) {
        throw new UnsupportedOperationException(getClass().getName() + " cannot take input");
    }

    @Override
    public Page getOutput() {

        Page page = source.getNextPage();
        if (page != null) {
            // assure the page is in memory before handing to another operator
            page = page.getLoadedPage();

            // update operator stats
            operatorContext.recordPhysicalInputWithTiming(
                    page.getSizeInBytes(),
                    page.getPositionCount(),
                    0);
            operatorContext.recordProcessedInput(page.getSizeInBytes(), page.getPositionCount());

        }

        // updating system memory usage should happen after page is loaded.
        systemMemoryContext.setBytes(0);
        operatorContext.setLatestMetrics(Metrics.EMPTY);
        return page;
    }
}
