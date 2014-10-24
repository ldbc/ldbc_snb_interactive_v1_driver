package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.Db;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.WorkloadStreams.WorkloadStreamDefinition;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.GlobalCompletionTimeReader;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.TimeSource;

import java.util.concurrent.atomic.AtomicBoolean;

class PreciseIndividualAsyncOperationStreamExecutorServiceThread extends Thread {
    private static final long POLL_INTERVAL_WHILE_WAITING_FOR_LAST_HANDLER_TO_FINISH_AS_MILLI = 100;

    private final OperationHandlerExecutor operationHandlerExecutor;
    private final ConcurrentErrorReporter errorReporter;
    private final AtomicBoolean hasFinished;
    private final AtomicBoolean forcedTerminate;
    private final DependencyAndNonDependencyHandlersRetriever dependencyAndNonDependencyHandlersRetriever;

    public PreciseIndividualAsyncOperationStreamExecutorServiceThread(TimeSource timeSource,
                                                                      OperationHandlerExecutor operationHandlerExecutor,
                                                                      ConcurrentErrorReporter errorReporter,
                                                                      WorkloadStreamDefinition streamDefinition,
                                                                      AtomicBoolean hasFinished,
                                                                      Spinner spinner,
                                                                      AtomicBoolean forcedTerminate,
                                                                      Db db,
                                                                      LocalCompletionTimeWriter localCompletionTimeWriter,
                                                                      GlobalCompletionTimeReader globalCompletionTimeReader,
                                                                      ConcurrentMetricsService metricsService) {
        super(PreciseIndividualAsyncOperationStreamExecutorServiceThread.class.getSimpleName() + "-" + System.currentTimeMillis());
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
        try {
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
                    String errMsg = String.format("Error encountered while submitting operation for execution\n%s\n%s",
                            handler.operation(),
                            ConcurrentErrorReporter.stackTraceToString(e));
                    errorReporter.reportError(this, errMsg);
                    continue;
                }
            }

            awaitAllRunningHandlers();
            this.hasFinished.set(true);
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    private void awaitAllRunningHandlers() {
        long pollInterval = POLL_INTERVAL_WHILE_WAITING_FOR_LAST_HANDLER_TO_FINISH_AS_MILLI;
        while (true) {
            if (0 == operationHandlerExecutor.uncompletedOperationHandlerCount()) break;
            if (forcedTerminate.get()) break;
            Spinner.powerNap(pollInterval);
        }
    }
}
