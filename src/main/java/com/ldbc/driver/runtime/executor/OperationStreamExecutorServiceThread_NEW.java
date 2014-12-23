package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.Db;
import com.ldbc.driver.OperationHandlerRunnableContext;
import com.ldbc.driver.WorkloadStreams.WorkloadStreamDefinition;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.GlobalCompletionTimeReader;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.TimeSource;

import java.util.concurrent.atomic.AtomicBoolean;

class OperationStreamExecutorServiceThread_NEW extends Thread {
    private static final long POLL_INTERVAL_WHILE_WAITING_FOR_LAST_HANDLER_TO_FINISH_AS_MILLI = 100;

    private final OperationHandlerExecutor operationHandlerExecutor;
    private final ConcurrentErrorReporter errorReporter;
    private final AtomicBoolean hasFinished;
    private final AtomicBoolean forcedTerminate;
    private final InitiatedTimeSubmittingOperationRetriever_NEW dependencyAndNonDependencyHandlersRetriever;

    public OperationStreamExecutorServiceThread_NEW(TimeSource timeSource,
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
        super(OperationStreamExecutorServiceThread_NEW.class.getSimpleName() + "-" + System.currentTimeMillis());
        this.operationHandlerExecutor = operationHandlerExecutor;
        this.errorReporter = errorReporter;
        this.hasFinished = hasFinished;
        this.forcedTerminate = forcedTerminate;
        this.dependencyAndNonDependencyHandlersRetriever = new InitiatedTimeSubmittingOperationRetriever_NEW(
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
            while (dependencyAndNonDependencyHandlersRetriever.hasNextHandlerRunner() && false == forcedTerminate.get()) {
                OperationHandlerRunnableContext handlerRunner;
                try {
                    handlerRunner = dependencyAndNonDependencyHandlersRetriever.nextHandlerRunner();
                } catch (Throwable e) {
                    String errMsg = String.format("Error while retrieving next handler\n%s",
                            ConcurrentErrorReporter.stackTraceToString(e));
                    errorReporter.reportError(this, errMsg);
                    break;
                }

                try {
                    // --- BLOCKING CALL (when bounded queue is full) ---
                    operationHandlerExecutor.execute(handlerRunner);
                } catch (OperationHandlerExecutorException e) {
                    String errMsg = String.format("Error encountered while submitting operation for execution\n%s\n%s",
                            handlerRunner.operation(),
                            ConcurrentErrorReporter.stackTraceToString(e));
                    errorReporter.reportError(this, errMsg);
                    break;
                }
            }

            awaitAllRunningHandlers();
            this.hasFinished.set(true);
        } catch (Throwable e) {
            errorReporter.reportError(this, ConcurrentErrorReporter.stackTraceToString(e));
            this.hasFinished.set(true);
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