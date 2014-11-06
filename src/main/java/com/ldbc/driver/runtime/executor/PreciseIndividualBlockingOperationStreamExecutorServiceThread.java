package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.Db;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.GlobalCompletionTimeReader;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.TimeSource;

import java.util.concurrent.atomic.AtomicBoolean;

class PreciseIndividualBlockingOperationStreamExecutorServiceThread extends Thread {
    private static final long POLL_INTERVAL_WHILE_WAITING_FOR_LAST_HANDLER_TO_FINISH_AS_MILLI = 100;

    private final OperationHandlerExecutor operationHandlerExecutor;
    private final ConcurrentErrorReporter errorReporter;
    private final AtomicBoolean hasFinished;
    private final AtomicBoolean forcedTerminate;
    private final DependencyAndNonDependencyHandlersRetriever dependencyAndNonDependencyHandlersRetriever;

    public PreciseIndividualBlockingOperationStreamExecutorServiceThread(TimeSource timeSource,
                                                                         OperationHandlerExecutor operationHandlerExecutor,
                                                                         ConcurrentErrorReporter errorReporter,
                                                                         WorkloadStreams.WorkloadStreamDefinition streamDefinition,
                                                                         AtomicBoolean hasFinished,
                                                                         Spinner spinner,
                                                                         AtomicBoolean forcedTerminate,
                                                                         Db db,
                                                                         LocalCompletionTimeWriter localCompletionTimeWriter,
                                                                         GlobalCompletionTimeReader globalCompletionTimeReader,
                                                                         ConcurrentMetricsService metricsService) {
        super(PreciseIndividualBlockingOperationStreamExecutorServiceThread.class.getSimpleName() + "-" + System.currentTimeMillis());
        this.operationHandlerExecutor = operationHandlerExecutor;
        this.errorReporter = errorReporter;
        this.hasFinished = hasFinished;
        this.forcedTerminate = forcedTerminate;
        this.dependencyAndNonDependencyHandlersRetriever = new DependencyAndNonDependencyHandlersRetriever(
                streamDefinition,
                db,
                localCompletionTimeWriter,
                globalCompletionTimeReader,
                spinner,
                timeSource,
                errorReporter,
                metricsService);
    }

    @Override
    public void run() {
        while (dependencyAndNonDependencyHandlersRetriever.hasNextHandler() && false == forcedTerminate.get()) {
            OperationHandler<?> handler;
            try {
                handler = dependencyAndNonDependencyHandlersRetriever.nextHandler();
            } catch (Exception e) {
                String errMsg = String.format("Error while retrieving next handler\n%s",
                        ConcurrentErrorReporter.stackTraceToString(e));
                errorReporter.reportError(this, errMsg);
                continue;
            }

            try {
                // --- BLOCKING CALL (when bounded queue is full) ---
                operationHandlerExecutor.execute(handler);
            } catch (OperationHandlerExecutorException e) {
                errorReporter.reportError(
                        this,
                        String.format("Error encountered while submitting operation for execution\nOperation: %s\n%s", handler.operation(), ConcurrentErrorReporter.stackTraceToString(e))
                );
            }

        }
        // Wait for final operation handler
        awaitExecutingHandler();
        this.hasFinished.set(true);
    }

    private void awaitExecutingHandler() {
        long pollInterval = POLL_INTERVAL_WHILE_WAITING_FOR_LAST_HANDLER_TO_FINISH_AS_MILLI;
        while (true) {
            if (operationHandlerExecutor.uncompletedOperationHandlerCount() == 0) break;
            if (forcedTerminate.get()) break;
            Spinner.powerNap(pollInterval);
        }
    }
}
