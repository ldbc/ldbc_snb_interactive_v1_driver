package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.*;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.GlobalCompletionTimeReader;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.TimeSource;

import java.util.concurrent.atomic.AtomicLong;

public class SameThreadOperationHandlerExecutor_NEW implements OperationExecutor_NEW {
    private final AtomicLong uncompletedHandlers = new AtomicLong(0);
    private final OperationHandlerRunnableContextRetriever operationHandlerRunnableContextInitializer;
    private final ChildOperationGenerator childOperationGenerator;

    public SameThreadOperationHandlerExecutor_NEW(Db db,
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
    public final void execute(Operation operation) throws OperationHandlerExecutorException {
        uncompletedHandlers.incrementAndGet();
        try {
            OperationHandlerRunnableContext operationHandlerRunnableContext =
                    operationHandlerRunnableContextInitializer.getInitializedHandlerFor(operation);
            operationHandlerRunnableContext.run();
            if (null != childOperationGenerator) {
                OperationResultReport resultReport = operationHandlerRunnableContext.operationResultReport();
                double state = childOperationGenerator.initialState();
                while (childOperationGenerator.hasNext(state)) {
                    Operation childOperation = childOperationGenerator.nextOperation(resultReport);
                    OperationHandlerRunnableContext childOperationHandlerRunnableContext =
                            operationHandlerRunnableContextInitializer.getInitializedHandlerFor(childOperation);
                    childOperationHandlerRunnableContext.run();
                    state = childOperationGenerator.updateState(state);
                    resultReport = childOperationHandlerRunnableContext.operationResultReport();
                    childOperationHandlerRunnableContext.cleanup();
                }
                operationHandlerRunnableContext.cleanup();
            }
        } catch (Throwable e) {
            throw new OperationHandlerExecutorException(
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
    synchronized public final void shutdown(long waitAsMilli) throws OperationHandlerExecutorException {
    }

    @Override
    public long uncompletedOperationHandlerCount() {
        return uncompletedHandlers.get();
    }
}
