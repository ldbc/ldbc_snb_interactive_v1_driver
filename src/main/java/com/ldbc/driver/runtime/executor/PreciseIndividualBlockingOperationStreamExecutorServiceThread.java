package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.*;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.DummyLocalCompletionTimeWriter;
import com.ldbc.driver.runtime.coordination.GlobalCompletionTimeReader;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.GctDependencyCheck;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

class PreciseIndividualBlockingOperationStreamExecutorServiceThread extends Thread {
    private static final Duration POLL_INTERVAL_WHILE_WAITING_FOR_LAST_HANDLER_TO_FINISH = Duration.fromMilli(100);
    private static final LocalCompletionTimeWriter DUMMY_LOCAL_COMPLETION_TIME_WRITER = new DummyLocalCompletionTimeWriter();

    private final TimeSource timeSource;
    private final OperationHandlerExecutor operationHandlerExecutor;
    private final Spinner spinner;
    private final ConcurrentErrorReporter errorReporter;
    private final Iterator<Operation<?>> operations;
    private final AtomicBoolean hasFinished;
    private final AtomicBoolean forcedTerminate;
    private final Map<Class<? extends Operation>, OperationClassification> operationClassifications;
    private final Db db;
    private final LocalCompletionTimeWriter localCompletionTimeWriter;
    private final GlobalCompletionTimeReader globalCompletionTimeReader;
    private final ConcurrentMetricsService metricsService;
    private final Duration durationToWaitForAllHandlersToFinishBeforeShutdown;

    public PreciseIndividualBlockingOperationStreamExecutorServiceThread(TimeSource timeSource,
                                                                         OperationHandlerExecutor operationHandlerExecutor,
                                                                         ConcurrentErrorReporter errorReporter,
                                                                         Iterator<Operation<?>> operations,
                                                                         AtomicBoolean hasFinished,
                                                                         Spinner spinner,
                                                                         AtomicBoolean forcedTerminate,
                                                                         Map<Class<? extends Operation>, OperationClassification> operationClassifications,
                                                                         Db db,
                                                                         LocalCompletionTimeWriter localCompletionTimeWriter,
                                                                         GlobalCompletionTimeReader globalCompletionTimeReader,
                                                                         ConcurrentMetricsService metricsService,
                                                                         Duration durationToWaitForAllHandlersToFinishBeforeShutdown) {
        super(PreciseIndividualBlockingOperationStreamExecutorServiceThread.class.getSimpleName() + "-" + System.currentTimeMillis());
        this.timeSource = timeSource;
        this.operationHandlerExecutor = operationHandlerExecutor;
        this.spinner = spinner;
        this.errorReporter = errorReporter;
        this.operations = operations;
        this.hasFinished = hasFinished;
        this.forcedTerminate = forcedTerminate;
        this.operationClassifications = operationClassifications;
        this.db = db;
        this.localCompletionTimeWriter = localCompletionTimeWriter;
        this.globalCompletionTimeReader = globalCompletionTimeReader;
        this.metricsService = metricsService;
        this.durationToWaitForAllHandlersToFinishBeforeShutdown = durationToWaitForAllHandlersToFinishBeforeShutdown;
    }

    @Override
    public void run() {
        long startTimeOfLastOperationAsNano = 0;
        while (operations.hasNext() && false == forcedTerminate.get()) {
            Operation<?> operation = operations.next();

            startTimeOfLastOperationAsNano = operation.scheduledStartTime().asNano();

            // get handler
            OperationHandler<?> handler;
            try {
                handler = getAndInitializeHandler(operation);
            } catch (OperationHandlerExecutorException e) {
                errorReporter.reportError(
                        this,
                        String.format("Error while retrieving handler for operation\n%s", ConcurrentErrorReporter.stackTraceToString(e)));
                continue;
            }

            // submit initiated time as soon as possible so GCT/dependencies can advance as soon as possible
            try {
                submitInitiatedTime(handler);
            } catch (OperationHandlerExecutorException e) {
                errorReporter.reportError(
                        this,
                        String.format("Error encountered while submitted Initiated Time\n%s", ConcurrentErrorReporter.stackTraceToString(e)));
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
        boolean executingHandlerFinishedInTime = awaitExecutingHandler(Time.fromNano(startTimeOfLastOperationAsNano), durationToWaitForAllHandlersToFinishBeforeShutdown);
        if (false == executingHandlerFinishedInTime) {
            errorReporter.reportError(this, "Last handler did not complete in time");
        }
        this.hasFinished.set(true);
    }

    private OperationHandler<?> getAndInitializeHandler(Operation<?> operation) throws OperationHandlerExecutorException {
        OperationHandler<?> operationHandler;
        try {
            operationHandler = db.getOperationHandler(operation);
        } catch (DbException e) {
            throw new OperationHandlerExecutorException(String.format("Error while retrieving handler for operation\nOperation: %s", operation));
        }

        try {
            LocalCompletionTimeWriter localCompletionTimeWriterForHandler = (isDependencyWritingOperation(operation))
                    ? localCompletionTimeWriter
                    : DUMMY_LOCAL_COMPLETION_TIME_WRITER;
            operationHandler.init(timeSource, spinner, operation, localCompletionTimeWriterForHandler, errorReporter, metricsService);
        } catch (OperationException e) {
            throw new OperationHandlerExecutorException(String.format("Error while initializing handler for operation\nOperation: %s", operation));
        }

        if (isDependencyReadingOperation(operation))
            operationHandler.addBeforeExecuteCheck(new GctDependencyCheck(globalCompletionTimeReader, operation, errorReporter));

        return operationHandler;
    }

    private boolean isDependencyWritingOperation(Operation<?> operation) {
        return operationClassifications.get(operation.getClass()).dependencyMode().equals(OperationClassification.DependencyMode.READ_WRITE);
    }

    private boolean isDependencyReadingOperation(Operation<?> operation) {
        return operationClassifications.get(operation.getClass()).dependencyMode().equals(OperationClassification.DependencyMode.READ_WRITE);
    }

    private void submitInitiatedTime(OperationHandler<?> handler) throws OperationHandlerExecutorException {
        try {
            handler.localCompletionTimeWriter().submitLocalInitiatedTime(handler.operation().scheduledStartTime());
        } catch (CompletionTimeException e) {
            throw new OperationHandlerExecutorException(
                    String.format("Error encountered while submitted Initiated Time for:\nOperation: %s", handler.operation()), e);
        }
    }

    private boolean awaitExecutingHandler(Time startTimeOfLastOperation, Duration timeoutDuration) {
        long pollInterval = POLL_INTERVAL_WHILE_WAITING_FOR_LAST_HANDLER_TO_FINISH.asMilli();
        long timeoutTimeMs = startTimeOfLastOperation.plus(timeoutDuration).asMilli();
        while (timeSource.nowAsMilli() < timeoutTimeMs) {
            if (operationHandlerExecutor.uncompletedOperationHandlerCount() == 0) return true;
            if (forcedTerminate.get()) return true;
            Spinner.powerNap(pollInterval);
        }
        return false;
    }
}
