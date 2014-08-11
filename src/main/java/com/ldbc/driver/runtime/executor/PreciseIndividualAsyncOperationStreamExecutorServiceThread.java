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
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.Function0;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class PreciseIndividualAsyncOperationStreamExecutorServiceThread extends Thread {
    // TODO this value should be configurable, or an entirely better policy should be used
    private static final Duration DURATION_TO_WAIT_FOR_ALL_HANDLERS_TO_FINISH = Duration.fromMinutes(30);
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
    private final AtomicInteger runningHandlerCount = new AtomicInteger(0);
    private final Map<Class<? extends Operation>, OperationClassification> operationClassifications;
    private final Db db;
    private final LocalCompletionTimeWriter localCompletionTimeWriter;
    private final GlobalCompletionTimeReader globalCompletionTimeReader;
    private final ConcurrentMetricsService metricsService;

    public PreciseIndividualAsyncOperationStreamExecutorServiceThread(TimeSource timeSource,
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
        super(PreciseIndividualAsyncOperationStreamExecutorServiceThread.class.getSimpleName() + "-" + System.currentTimeMillis());
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
        while (operations.hasNext() && false == forcedTerminate.get()) {
            Operation<?> operation = operations.next();

            // get handler
            OperationHandler<?> handler;
            try {
                handler = getAndInitializeHandler(operation);
            } catch (OperationHandlerExecutorException e) {
                errorReporter.reportError(
                        this,
                        String.format("Error while retrieving handler for operation\nOperation: %s\n%s",
                                operation,
                                ConcurrentErrorReporter.stackTraceToString(e)));
                continue;
            }

            // submit initiated time as soon as possible so GCT/dependencies can advance as soon as possible
            try {
                handler.localCompletionTimeWriter().submitLocalInitiatedTime(handler.operation().scheduledStartTime());
            } catch (CompletionTimeException e) {
                errorReporter.reportError(this,
                        String.format("Error encountered while submitted Initiated Time for:\n\t%s\n%s",
                                handler.operation().toString(),
                                ConcurrentErrorReporter.stackTraceToString(e)));
                continue;
            }

            // Schedule slightly early to account for context switch - internally, handler will schedule at exact start time
            // TODO forcedTerminate does not cover all cases at present this spin loop is still blocking -> inject a check that throws exception?
            // TODO or SpinnerChecks have three possible results? (TRUE, NOT_TRUE_YET, FALSE)
            // TODO and/or Spinner has an emergency terminate button?
            slightlyEarlySpinner.waitForScheduledStartTime(handler.operation());

            // execute handler
            try {
                executeHandler(handler);
            } catch (OperationHandlerExecutorException e) {
                String errMsg = String.format("Error encountered while submitting operation for execution\n%s",
                        ConcurrentErrorReporter.stackTraceToString(e));
                errorReporter.reportError(this, errMsg);
                continue;
            }
        }
        boolean handlersFinishedInTime = awaitAllRunningHandlers(DURATION_TO_WAIT_FOR_ALL_HANDLERS_TO_FINISH);
        if (false == handlersFinishedInTime) {
            errorReporter.reportError(this, String.format("At least one operation handler did not complete in time"));
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

        OperationClassification.DependencyMode operationDependencyMode = operationClassifications.get(operation.getClass()).dependencyMode();
        try {
            LocalCompletionTimeWriter localCompletionTimeWriterForHandler = (isDependencyWritingOperation(operationDependencyMode))
                    ? localCompletionTimeWriter
                    : DUMMY_LOCAL_COMPLETION_TIME_WRITER;
            operationHandler.init(TIME_SOURCE, spinner, operation, localCompletionTimeWriterForHandler, errorReporter, metricsService);
        } catch (OperationException e) {
            throw new OperationHandlerExecutorException(String.format("Error while initializing handler for operation\nOperation: %s", operation));
        }

        if (isDependencyReadingOperation(operationDependencyMode))
            operationHandler.addBeforeExecuteCheck(new GctDependencyCheck(globalCompletionTimeReader, operation, errorReporter));

        return operationHandler;
    }

    private boolean isDependencyWritingOperation(OperationClassification.DependencyMode operationDependencyMode) {
        return operationDependencyMode.equals(OperationClassification.DependencyMode.READ_WRITE);
    }

    private boolean isDependencyReadingOperation(OperationClassification.DependencyMode operationDependencyMode) {
        return operationDependencyMode.equals(OperationClassification.DependencyMode.READ_WRITE) ||
                operationDependencyMode.equals(OperationClassification.DependencyMode.READ);
    }

    private void executeHandler(OperationHandler<?> handler) throws OperationHandlerExecutorException {
        try {
            runningHandlerCount.incrementAndGet();
            DecrementRunningHandlerCountFun decrementRunningHandlerCountFun = new DecrementRunningHandlerCountFun(runningHandlerCount);
            handler.addOnCompleteTask(decrementRunningHandlerCountFun);
            operationHandlerExecutor.execute(handler);
        } catch (OperationHandlerExecutorException e) {
            throw new OperationHandlerExecutorException(
                    String.format("Error encountered while submitting operation for execution\nOperation: %s", handler.operation()));
        }
    }

    private boolean awaitAllRunningHandlers(Duration timeoutDuration) {
        long pollInterval = POLL_INTERVAL_WHILE_WAITING_FOR_LAST_HANDLER_TO_FINISH.asMilli();
        long timeoutTimeMs = TIME_SOURCE.now().plus(timeoutDuration).asMilli();
        while (TIME_SOURCE.nowAsMilli() < timeoutTimeMs) {
            if (allHandlersCompleted()) return true;
            if (forcedTerminate.get()) return true;
            Spinner.powerNap(pollInterval);
        }
        return false;
    }

    private boolean allHandlersCompleted() {
        return 0 == runningHandlerCount.get();
    }

    private final class DecrementRunningHandlerCountFun implements Function0 {
        private final AtomicInteger runningHandlerCount;

        private DecrementRunningHandlerCountFun(AtomicInteger runningHandlerCount) {
            this.runningHandlerCount = runningHandlerCount;
        }

        @Override
        public Object apply() {
            runningHandlerCount.decrementAndGet();
            return null;
        }
    }
}
