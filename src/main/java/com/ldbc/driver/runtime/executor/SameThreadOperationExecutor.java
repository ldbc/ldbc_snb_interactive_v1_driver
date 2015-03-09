package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.*;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.GlobalCompletionTimeReader;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.TimeSource;

import java.util.concurrent.atomic.AtomicLong;

public class SameThreadOperationExecutor implements OperationExecutor {
    private final AtomicLong uncompletedHandlers = new AtomicLong(0);
    private final OperationHandlerRunnableContextRetriever operationHandlerRunnableContextInitializer;
    private final ChildOperationGenerator childOperationGenerator;

    public SameThreadOperationExecutor(Db db,
                                       WorkloadStreams.WorkloadStreamDefinition streamDefinition,
                                       LocalCompletionTimeWriter localCompletionTimeWriter,
                                       GlobalCompletionTimeReader globalCompletionTimeReader,
                                       Spinner spinner,
                                       TimeSource timeSource,
                                       ConcurrentErrorReporter errorReporter,
                                       ConcurrentMetricsService metricsService,
                                       ChildOperationGenerator childOperationGenerator) {
        this.childOperationGenerator = childOperationGenerator;
        this.operationHandlerRunnableContextInitializer = new OperationHandlerRunnableContextRetriever(
                streamDefinition,
                db,
                localCompletionTimeWriter,
                globalCompletionTimeReader,
                spinner,
                timeSource,
                errorReporter,
                metricsService
        );
    }

    @Override
    public final void execute(Operation operation) throws OperationExecutorException {
        uncompletedHandlers.incrementAndGet();
        try {
            OperationHandlerRunnableContext operationHandlerRunnableContext =
                    operationHandlerRunnableContextInitializer.getInitializedHandlerFor(operation);
            operationHandlerRunnableContext.run();
            if (null != childOperationGenerator) {
                Object result = operationHandlerRunnableContext.resultReporter().result();
                double state = childOperationGenerator.initialState();
                while (null != (operation = childOperationGenerator.nextOperation(state, operation, result))) {
                    OperationHandlerRunnableContext childOperationHandlerRunnableContext =
                            operationHandlerRunnableContextInitializer.getInitializedHandlerFor(operation);
                    childOperationHandlerRunnableContext.run();
                    result = childOperationHandlerRunnableContext.resultReporter().result();
                    childOperationHandlerRunnableContext.cleanup();
                    state = childOperationGenerator.updateState(state, operation.type());
                }
            }
            operationHandlerRunnableContext.cleanup();
        } catch (Throwable e) {
            throw new OperationExecutorException(
                    String.format("Error retrieving handler\nOperation: %s\n%s",
                            operation,
                            ConcurrentErrorReporter.stackTraceToString(e)),
                    e
            );
        } finally {
            uncompletedHandlers.decrementAndGet();
        }
    }

    @Override
    synchronized public final void shutdown(long waitAsMilli) throws OperationExecutorException {
    }

    @Override
    public long uncompletedOperationHandlerCount() {
        return uncompletedHandlers.get();
    }
}
