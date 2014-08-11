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
import com.ldbc.driver.runtime.scheduling.SpinnerCheck;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.Function0;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

class PreciseIndividualBlockingOperationStreamExecutorServiceThread extends Thread {
    // TODO this value should be configurable, or an entirely better policy should be used
    private static final Duration DURATION_TO_WAIT_FOR_LAST_HANDLER_TO_FINISH = Duration.fromMinutes(30);
    private static final Duration POLL_INTERVAL_WHILE_WAITING_FOR_LAST_HANDLER_TO_FINISH = Duration.fromMilli(100);
    private static final LocalCompletionTimeWriter DUMMY_LOCAL_COMPLETION_TIME_WRITER = new DummyLocalCompletionTimeWriter();

    private final TimeSource TIME_SOURCE;
    private final OperationHandlerExecutor operationHandlerExecutor;
    private final Spinner spinner;
    private final Spinner slightlyEarlySpinner;
    private final ConcurrentErrorReporter errorReporter;
    private final Iterator<Operation<?>> operations;
    private final AtomicBoolean hasFinished;
    private final AtomicBoolean forcedTerminate;
    private final Map<Class<? extends Operation>, OperationClassification> operationClassifications;
    private final Db db;
    private final LocalCompletionTimeWriter localCompletionTimeWriter;
    private final GlobalCompletionTimeReader globalCompletionTimeReader;
    private final ConcurrentMetricsService metricsService;

    public PreciseIndividualBlockingOperationStreamExecutorServiceThread(TimeSource timeSource,
                                                                         OperationHandlerExecutor operationHandlerExecutor,
                                                                         ConcurrentErrorReporter errorReporter,
                                                                         Iterator<Operation<?>> operations,
                                                                         AtomicBoolean hasFinished,
                                                                         Spinner spinner,
                                                                         Spinner slightlyEarlySpinner,
                                                                         AtomicBoolean forcedTerminate,
                                                                         Map<Class<? extends Operation>, OperationClassification> operationClassifications,
                                                                         Db db,
                                                                         LocalCompletionTimeWriter localCompletionTimeWriter,
                                                                         GlobalCompletionTimeReader globalCompletionTimeReader,
                                                                         ConcurrentMetricsService metricsService) {
        super(PreciseIndividualBlockingOperationStreamExecutorServiceThread.class.getSimpleName() + "-" + System.currentTimeMillis());
        this.TIME_SOURCE = timeSource;
        this.operationHandlerExecutor = operationHandlerExecutor;
        this.spinner = spinner;
        this.slightlyEarlySpinner = slightlyEarlySpinner;
        this.errorReporter = errorReporter;
        this.operations = operations;
        this.hasFinished = hasFinished;
        this.forcedTerminate = forcedTerminate;
        this.operationClassifications = operationClassifications;
        this.db = db;
        this.localCompletionTimeWriter = localCompletionTimeWriter;
        this.globalCompletionTimeReader = globalCompletionTimeReader;
        this.metricsService = metricsService;
    }

    @Override
    public void run() {
        PreviousHandlerCompletedCheck previousHandlerCompletedCheck = new PreviousHandlerCompletedCheck(new AtomicBoolean(true), null);
        while (operations.hasNext() && false == forcedTerminate.get()) {
            Operation<?> operation = operations.next();

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

            // Ensures previously executed handler has completed before handler starts executing
            handler.addBeforeExecuteCheck(previousHandlerCompletedCheck);

            // submit initiated time as soon as possible so GCT/dependencies can advance as soon as possible
            try {
                submitInitiatedTime(handler);
            } catch (OperationHandlerExecutorException e) {
                errorReporter.reportError(
                        this,
                        String.format("Error encountered while submitted Initiated Time\n%s", ConcurrentErrorReporter.stackTraceToString(e)));
                continue;
            }

            // Schedule slightly early to account for context switch - internally, handler will schedule at exact start time
            // TODO forcedTerminate does not cover all cases at present this spin loop is still blocking -> inject a check that throws exception?
            // TODO or SpinnerChecks have three possible results? (TRUE, NOT_TRUE_YET, FALSE)
            // TODO and/or Spinner has an emergency terminate button?
            slightlyEarlySpinner.waitForScheduledStartTime(handler.operation());

            // execute handler
            try {
                previousHandlerCompletedCheck = executeHandler(handler);
            } catch (OperationHandlerExecutorException e) {
                errorReporter.reportError(
                        this,
                        String.format("Error encountered while submitting operation for execution\n%s", ConcurrentErrorReporter.stackTraceToString(e)));
                continue;
            }
        }
        // Wait for final operation handler
        boolean executingHandlerFinishedInTime = awaitExecutingHandler(DURATION_TO_WAIT_FOR_LAST_HANDLER_TO_FINISH, previousHandlerCompletedCheck);
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
            operationHandler.init(TIME_SOURCE, spinner, operation, localCompletionTimeWriterForHandler, errorReporter, metricsService);
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

    private PreviousHandlerCompletedCheck executeHandler(OperationHandler<?> handler) throws OperationHandlerExecutorException {
        try {
            final AtomicBoolean handlerHasCompleted = new AtomicBoolean(false);
            handler.addOnCompleteTask(new SetHandlerHasCompletedFlagFun(handlerHasCompleted));
            operationHandlerExecutor.execute(handler);
            return new PreviousHandlerCompletedCheck(handlerHasCompleted, handler.operation());
        } catch (OperationHandlerExecutorException e) {
            throw new OperationHandlerExecutorException(
                    String.format("Error encountered while submitting operation for execution\nOperation: %s", handler.operation()), e);
        }
    }

    private boolean awaitExecutingHandler(Duration timeoutDuration, PreviousHandlerCompletedCheck previousHandlerCompletedCheck) {
        long pollInterval = POLL_INTERVAL_WHILE_WAITING_FOR_LAST_HANDLER_TO_FINISH.asMilli();
        long timeoutTimeMs = TIME_SOURCE.now().plus(timeoutDuration).asMilli();
        while (TIME_SOURCE.nowAsMilli() < timeoutTimeMs) {
            if (previousHandlerCompletedCheck.doCheck()) return true;
            if (forcedTerminate.get()) return true;
            Spinner.powerNap(pollInterval);
        }
        return false;
    }

    private final class PreviousHandlerCompletedCheck implements SpinnerCheck {
        private final AtomicBoolean handlerHasCompleted;
        private final Operation<?> previousOperation;

        private PreviousHandlerCompletedCheck(AtomicBoolean previousHandlerHasCompleted, Operation<?> previousOperation) {
            this.handlerHasCompleted = previousHandlerHasCompleted;
            this.previousOperation = previousOperation;
        }

        @Override
        public boolean doCheck() {
            return handlerHasCompleted.get();
        }

        @Override
        public boolean handleFailedCheck(Operation<?> operation) {
            String errMsg = String.format(
                    "Previous operation did not complete in time for next synchronous operation to start\n"
                            + " Previous Operation (%s): %s\n"
                            + " Next Operation (%s): %s",
                    previousOperation.scheduledStartTime(),
                    previousOperation,
                    operation.scheduledStartTime(),
                    operation
            );
            errorReporter.reportError(this, errMsg);
            return false;
        }
    }

    private final class SetHandlerHasCompletedFlagFun implements Function0 {
        private final AtomicBoolean handlerHasCompleted;

        private SetHandlerHasCompletedFlagFun(AtomicBoolean handlerHasCompleted) {
            this.handlerHasCompleted = handlerHasCompleted;
        }

        @Override
        public Object apply() {
            handlerHasCompleted.set(true);
            return null;
        }
    }
}
