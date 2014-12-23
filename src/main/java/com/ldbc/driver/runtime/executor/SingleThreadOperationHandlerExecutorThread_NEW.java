package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.ChildOperationGenerator;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationHandlerRunnableContext;
import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.QueueEventFetcher;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class SingleThreadOperationHandlerExecutorThread_NEW extends Thread {
    private final QueueEventFetcher<Operation> operationQueueEventFetcher;
    private final ConcurrentErrorReporter errorReporter;
    private final AtomicLong uncompletedHandlers;
    private final AtomicBoolean forcedShutdownRequested = new AtomicBoolean(false);
    private final OperationHandlerRunnableContextRetriever operationHandlerRunnableContextRetriever;
    private final ChildOperationGenerator childOperationGenerator;

    SingleThreadOperationHandlerExecutorThread_NEW(Queue<Operation> operationHandlerRunnerQueue,
                                                   ConcurrentErrorReporter errorReporter,
                                                   AtomicLong uncompletedHandlers,
                                                   OperationHandlerRunnableContextRetriever operationHandlerRunnableContextRetriever,
                                                   ChildOperationGenerator childOperationGenerator) {
        super(SingleThreadOperationHandlerExecutorThread_NEW.class.getSimpleName() + "-" + System.currentTimeMillis());
        this.operationQueueEventFetcher = QueueEventFetcher.queueEventFetcherFor(operationHandlerRunnerQueue);
        this.errorReporter = errorReporter;
        this.uncompletedHandlers = uncompletedHandlers;
        this.operationHandlerRunnableContextRetriever = operationHandlerRunnableContextRetriever;
        this.childOperationGenerator = childOperationGenerator;
    }

    @Override
    public void run() {
        Operation operation = null;
        try {
            operation = operationQueueEventFetcher.fetchNextEvent();
            while (operation != SingleThreadOperationHandlerExecutor_NEW.TERMINATE_OPERATION && false == forcedShutdownRequested.get()) {
                OperationHandlerRunnableContext operationHandlerRunnableContext =
                        operationHandlerRunnableContextRetriever.getInitializedHandlerFor(operation);
                operationHandlerRunnableContext.run();
                if (null != childOperationGenerator) {
                    OperationResultReport resultReport = operationHandlerRunnableContext.operationResultReport();
                    operationHandlerRunnableContext.cleanup();
                    double state = childOperationGenerator.initialState();
                    while (childOperationGenerator.hasNext(state)) {
                        Operation childOperation = childOperationGenerator.nextOperation(resultReport);
                        OperationHandlerRunnableContext childOperationHandlerRunnableContext =
                                operationHandlerRunnableContextRetriever.getInitializedHandlerFor(childOperation);
                        childOperationHandlerRunnableContext.run();
                        resultReport = childOperationHandlerRunnableContext.operationResultReport();
                        childOperationHandlerRunnableContext.cleanup();
                        state = childOperationGenerator.updateState(state);
                    }
                }
                uncompletedHandlers.decrementAndGet();
                operation = operationQueueEventFetcher.fetchNextEvent();
            }
        } catch (Exception e) {
            errorReporter.reportError(
                    this,
                    String.format("Error retrieving handler\nOperation: %s\n%s",
                            operation,
                            ConcurrentErrorReporter.stackTraceToString(e))
            );
        }
    }

    void forceShutdown() {
        forcedShutdownRequested.set(true);
    }
}
