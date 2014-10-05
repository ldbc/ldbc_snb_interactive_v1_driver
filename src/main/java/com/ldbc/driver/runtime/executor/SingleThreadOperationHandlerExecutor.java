package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.DefaultQueues;
import com.ldbc.driver.runtime.QueueEventSubmitter;
import com.ldbc.driver.temporal.Duration;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class SingleThreadOperationHandlerExecutor implements OperationHandlerExecutor {
    static final OperationHandler<?> TERMINATE_HANDLER = new OperationHandler<Operation<?>>() {
        @Override
        protected OperationResultReport executeOperation(Operation<?> operation) throws DbException {
            return null;
        }
    };

    private final SingleThreadOperationHandlerExecutorThread executorThread;
    private final QueueEventSubmitter<OperationHandler<?>> operationHandlerQueueEventSubmitter;
    private final AtomicLong uncompletedHandlers = new AtomicLong(0);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    public SingleThreadOperationHandlerExecutor(ConcurrentErrorReporter errorReporter, int boundedQueueSize) {
        Queue<OperationHandler<?>> operationHandlerQueue = DefaultQueues.newAlwaysBlockingBounded(boundedQueueSize);
        this.operationHandlerQueueEventSubmitter = QueueEventSubmitter.queueEventSubmitterFor(operationHandlerQueue);
        this.executorThread = new SingleThreadOperationHandlerExecutorThread(operationHandlerQueue, errorReporter, uncompletedHandlers);
        this.executorThread.start();
    }

    @Override
    public final void execute(OperationHandler<?> operationHandler) throws OperationHandlerExecutorException {
        uncompletedHandlers.incrementAndGet();
        try {
            operationHandlerQueueEventSubmitter.submitEventToQueue(operationHandler);
        } catch (InterruptedException e) {
            throw new OperationHandlerExecutorException("Error encountered while submitting handler to queue", e);
        }
    }

    @Override
    synchronized public final void shutdown(Duration wait) throws OperationHandlerExecutorException {
        if (shutdown.get())
            throw new OperationHandlerExecutorException("Executor has already been shutdown");
        try {
            operationHandlerQueueEventSubmitter.submitEventToQueue(TERMINATE_HANDLER);
            executorThread.join(wait.asMilli());
            if (uncompletedHandlers.get() > 0) {
                executorThread.forceShutdown();
                throw new OperationHandlerExecutorException(String.format("Executor shutdown before all handlers could complete - %s uncompleted handlers", uncompletedHandlers));
            }
        } catch (Exception e) {
            throw new OperationHandlerExecutorException("Error encountered while trying to shutdown", e);
        }
        shutdown.set(true);
    }

    @Override
    public long uncompletedOperationHandlerCount() {
        return uncompletedHandlers.get();
    }
}
