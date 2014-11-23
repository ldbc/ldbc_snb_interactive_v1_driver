package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.OperationHandlerRunnableContext;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.DefaultQueues;
import com.ldbc.driver.runtime.QueueEventSubmitter;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class SingleThreadOperationHandlerExecutor implements OperationHandlerExecutor {
    static final OperationHandlerRunnableContext TERMINATE_HANDLER_RUNNER = new OperationHandlerRunnableContext();

    private final SingleThreadOperationHandlerExecutorThread executorThread;
    private final QueueEventSubmitter<OperationHandlerRunnableContext> operationHandlerQueueEventSubmitter;
    private final AtomicLong uncompletedHandlers = new AtomicLong(0);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    public SingleThreadOperationHandlerExecutor(ConcurrentErrorReporter errorReporter, int boundedQueueSize) {
        Queue<OperationHandlerRunnableContext> operationHandlerQueue = DefaultQueues.newAlwaysBlockingBounded(boundedQueueSize);
        this.operationHandlerQueueEventSubmitter = QueueEventSubmitter.queueEventSubmitterFor(operationHandlerQueue);
        this.executorThread = new SingleThreadOperationHandlerExecutorThread(operationHandlerQueue, errorReporter, uncompletedHandlers);
        this.executorThread.start();
    }

    @Override
    public final void execute(OperationHandlerRunnableContext operationHandlerRunner) throws OperationHandlerExecutorException {
        uncompletedHandlers.incrementAndGet();
        try {
            operationHandlerQueueEventSubmitter.submitEventToQueue(operationHandlerRunner);
        } catch (InterruptedException e) {
            throw new OperationHandlerExecutorException("Error encountered while submitting handler to queue", e);
        }
    }

    @Override
    synchronized public final void shutdown(long waitAsMilli) throws OperationHandlerExecutorException {
        if (shutdown.get())
            throw new OperationHandlerExecutorException("Executor has already been shutdown");
        try {
            operationHandlerQueueEventSubmitter.submitEventToQueue(TERMINATE_HANDLER_RUNNER);
            executorThread.join(waitAsMilli);
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
