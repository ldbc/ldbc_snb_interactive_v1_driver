package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.*;
import com.ldbc.driver.WorkloadStreams.WorkloadStreamDefinition;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.GlobalCompletionTimeReader;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;

import java.util.concurrent.atomic.AtomicBoolean;

class PreciseIndividualAsyncOperationStreamExecutorServiceThread extends Thread {
    private static final Duration POLL_INTERVAL_WHILE_WAITING_FOR_LAST_HANDLER_TO_FINISH = Duration.fromMilli(100);

    private final TimeSource timeSource;
    private final OperationHandlerExecutor operationHandlerExecutor;
    private final ConcurrentErrorReporter errorReporter;
    private final AtomicBoolean hasFinished;
    private final AtomicBoolean forcedTerminate;
    private final DependencyAndNonDependencyHandlersRetriever dependencyAndNonDependencyHandlersRetriever;
    private final Duration durationToWaitForAllHandlersToFinishBeforeShutdown;

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
                                                                      ConcurrentMetricsService metricsService,
                                                                      Duration durationToWaitForAllHandlersToFinishBeforeShutdown) {
        super(PreciseIndividualAsyncOperationStreamExecutorServiceThread.class.getSimpleName() + "-" + System.currentTimeMillis());
        this.timeSource = timeSource;
        this.operationHandlerExecutor = operationHandlerExecutor;
        this.errorReporter = errorReporter;
        this.hasFinished = hasFinished;
        this.forcedTerminate = forcedTerminate;
        this.durationToWaitForAllHandlersToFinishBeforeShutdown = durationToWaitForAllHandlersToFinishBeforeShutdown;
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
        long startTimeOfLastOperationAsNano = 0;
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
            startTimeOfLastOperationAsNano = handler.operation().scheduledStartTime().asNano();

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

        boolean handlersFinishedInTime = awaitAllRunningHandlers(Time.fromNano(startTimeOfLastOperationAsNano), durationToWaitForAllHandlersToFinishBeforeShutdown);
        if (false == handlersFinishedInTime) {
            errorReporter.reportError(
                    this,
                    String.format(
                            "%s operation handlers did not complete in time (within %s of the time the last operation was submitted for execution)",
                            operationHandlerExecutor.uncompletedOperationHandlerCount(),
                            durationToWaitForAllHandlersToFinishBeforeShutdown
                    )
            );
        }
        this.hasFinished.set(true);
    }

    private boolean awaitAllRunningHandlers(Time startTimeOfLastOperation, Duration timeoutDuration) {
        long pollInterval = POLL_INTERVAL_WHILE_WAITING_FOR_LAST_HANDLER_TO_FINISH.asMilli();
        long timeoutTimeMs = startTimeOfLastOperation.plus(timeoutDuration).asMilli();
        while (timeSource.nowAsMilli() < timeoutTimeMs) {
            if (0 == operationHandlerExecutor.uncompletedOperationHandlerCount()) return true;
            if (forcedTerminate.get()) return true;
            Spinner.powerNap(pollInterval);
        }
        return false;
    }

}
