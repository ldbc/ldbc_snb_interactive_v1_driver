package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.*;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.generator.Window;
import com.ldbc.driver.generator.WindowGenerator;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.DummyLocalCompletionTimeWriter;
import com.ldbc.driver.runtime.coordination.GlobalCompletionTimeReader;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.*;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.Function0;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class UniformWindowedOperationStreamExecutorServiceThread extends Thread {
    // TODO this value should be configurable, or an entirely better policy should be used
    private static final Duration DURATION_TO_WAIT_FOR_LAST_HANDLER_TO_FINISH = Duration.fromMinutes(30);
    private static final Duration POLL_INTERVAL_WHILE_WAITING_FOR_LAST_HANDLER_TO_FINISH = Duration.fromMilli(100);
    private static final LocalCompletionTimeWriter DUMMY_LOCAL_COMPLETION_TIME_WRITER = new DummyLocalCompletionTimeWriter();

    private final TimeSource TIME_SOURCE;
    private final OperationHandlerExecutor operationHandlerExecutor;
    private final Scheduler<List<Operation<?>>, Window.OperationTimeRangeWindow> scheduler;
    private final Spinner spinner;
    private final Spinner slightlyEarlySpinner;
    private final ConcurrentErrorReporter errorReporter;
    private final AtomicBoolean hasFinished;
    private final WindowGenerator<Operation<?>, Window.OperationTimeRangeWindow> operationWindows;
    private final AtomicBoolean forcedTerminate;
    private final Db db;
    private final Map<Class<? extends Operation>, OperationClassification> operationClassifications;
    private final LocalCompletionTimeWriter localCompletionTimeWriter;
    private final GlobalCompletionTimeReader globalCompletionTimeReader;
    private final ConcurrentMetricsService metricsService;

    public UniformWindowedOperationStreamExecutorServiceThread(TimeSource timeSource,
                                                               final Time firstWindowStartTime,
                                                               final Duration windowSize,
                                                               OperationHandlerExecutor operationHandlerExecutor,
                                                               ConcurrentErrorReporter errorReporter,
                                                               Iterator<Operation<?>> operations,
                                                               AtomicBoolean hasFinished,
                                                               Spinner spinner,
                                                               Spinner slightlyEarlySpinner,
                                                               AtomicBoolean forcedTerminate,
                                                               Db db,
                                                               Map<Class<? extends Operation>, OperationClassification> operationClassifications,
                                                               LocalCompletionTimeWriter localCompletionTimeWriter,
                                                               GlobalCompletionTimeReader globalCompletionTimeReader,
                                                               ConcurrentMetricsService metricsService) {
        super(UniformWindowedOperationStreamExecutorServiceThread.class.getSimpleName() + System.currentTimeMillis());
        this.TIME_SOURCE = timeSource;
        this.operationHandlerExecutor = operationHandlerExecutor;
        this.scheduler = new UniformWindowedOperationScheduler();
        this.spinner = spinner;
        this.slightlyEarlySpinner = slightlyEarlySpinner;
        this.errorReporter = errorReporter;
        this.hasFinished = hasFinished;
        this.forcedTerminate = forcedTerminate;
        this.db = db;
        this.operationClassifications = operationClassifications;
        this.localCompletionTimeWriter = localCompletionTimeWriter;
        this.globalCompletionTimeReader = globalCompletionTimeReader;
        this.metricsService = metricsService;
        // generates windows with appropriate start and end times
        Generator<Window.OperationTimeRangeWindow> windows = new Generator<Window.OperationTimeRangeWindow>() {
            private Time windowStartTime = firstWindowStartTime;

            @Override
            protected Window.OperationTimeRangeWindow doNext() throws GeneratorException {
                Window.OperationTimeRangeWindow window = new Window.OperationTimeRangeWindow(windowStartTime, windowSize);
                windowStartTime = windowStartTime.plus(windowSize);
                return window;
            }
        };
        // removes windows of handlers, where every handler in each window is within the same time range/window
        this.operationWindows = new WindowGenerator<>(operations, windows, WindowGenerator.PartialWindowStrategy.RETURN);
    }

    @Override
    public void run() {
        Window.OperationTimeRangeWindow window = null;
        AtomicInteger numberOfExecutingHandlersFromPreviousWindow = new AtomicInteger(0);
        AtomicInteger numberOfExecutingHandlersFromCurrentWindow = new AtomicInteger(0);
        HandlersFromPreviousWindowHaveFinishedCheck handlersFromPreviousWindowHaveFinishedCheck;
        while (operationWindows.hasNext() && false == forcedTerminate.get()) {
            numberOfExecutingHandlersFromPreviousWindow = numberOfExecutingHandlersFromCurrentWindow;
            numberOfExecutingHandlersFromCurrentWindow = new AtomicInteger(0);
            handlersFromPreviousWindowHaveFinishedCheck = new HandlersFromPreviousWindowHaveFinishedCheck(
                    numberOfExecutingHandlersFromPreviousWindow,
                    errorReporter,
                    this);
            window = operationWindows.next();
            List<Operation<?>> rescheduledOperations = scheduler.schedule(window);

            List<OperationHandler<?>> operationHandlers = new ArrayList<>(rescheduledOperations.size());
            // submit initiated time as soon as possible so GCT/dependencies can advance as soon as possible
            for (int i = 0; i < rescheduledOperations.size(); i++) {
                numberOfExecutingHandlersFromCurrentWindow.incrementAndGet();
                Operation<?> operation = rescheduledOperations.get(i);

                // get handler
                OperationHandler<?> operationHandler;
                try {
                    operationHandler = getAndInitializeHandler(operation);
                } catch (OperationHandlerExecutorException e) {
                    errorReporter.reportError(
                            this,
                            String.format("Error while retrieving handler for operation\n%s", ConcurrentErrorReporter.stackTraceToString(e)));
                    continue;
                }

                // submit initiated time as soon as possible so GCT/dependencies can advance as soon as possible
                try {
                    submitInitiatedTime(operationHandler);
                } catch (OperationHandlerExecutorException e) {
                    errorReporter.reportError(
                            this,
                            String.format("Error encountered while submitted Initiated Time\n%s", ConcurrentErrorReporter.stackTraceToString(e)));
                    continue;
                }
                operationHandlers.add(operationHandler);
            }

            DecrementRunningHandlerCountFun decrementRunningHandlerCountFun =
                    new DecrementRunningHandlerCountFun(numberOfExecutingHandlersFromCurrentWindow);

            // execute operation handlers for current window
            for (int i = 0; i < operationHandlers.size(); i++) {
                OperationHandler<?> operationHandler = operationHandlers.get(i);
                // Schedule slightly early to account for context switch - internally, handler will schedule at exact start time
                // TODO forcedTerminate does not cover all cases at present this spin loop is still blocking -> inject a check that throws exception?
                // TODO or SpinnerChecks have three possible results? (TRUE, NOT_TRUE_YET, FALSE)
                // TODO and/or Spinner has an emergency terminate button?
                slightlyEarlySpinner.waitForScheduledStartTime(operationHandler.operation());

                // execute handler
                try {
                    executeHandler(
                            operationHandler,
                            handlersFromPreviousWindowHaveFinishedCheck,
                            decrementRunningHandlerCountFun);
                } catch (OperationHandlerExecutorException e) {
                    errorReporter.reportError(
                            this,
                            String.format("Error encountered while submitting operation for execution\n%s", ConcurrentErrorReporter.stackTraceToString(e)));
                    continue;
                }
            }
        }

        if (null != window) {
            long pollInterval = POLL_INTERVAL_WHILE_WAITING_FOR_LAST_HANDLER_TO_FINISH.asMilli();
            // long waitUntilTimeAsMilli = TIME_SOURCE.now().plus(DURATION_TO_WAIT_FOR_LAST_HANDLER_TO_FINISH).asMilli();
            long latestTimeToWaitAsMilli = window.windowEndTimeExclusive().asMilli();
            // wait for operations from last window to finish executing
            while (0 == numberOfExecutingHandlersFromPreviousWindow.get() && 0 == numberOfExecutingHandlersFromCurrentWindow.get()) {
                if (forcedTerminate.get()) break;
                if (TIME_SOURCE.nowAsMilli() > latestTimeToWaitAsMilli) {
                    errorReporter.reportError(this, "One or more handlers from the last window took too long to complete");
                    break;
                }
                Spinner.powerNap(pollInterval);
            }
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

    private void submitInitiatedTime(OperationHandler<?> operationHandler) throws OperationHandlerExecutorException {
        try {
            operationHandler.localCompletionTimeWriter().submitLocalInitiatedTime(operationHandler.operation().scheduledStartTime());
        } catch (CompletionTimeException e) {
            throw new OperationHandlerExecutorException(
                    String.format("Error encountered while submitted Initiated Time\nOperation: %s", operationHandler.operation()), e);
        }
    }

    private void executeHandler(OperationHandler<?> handler,
                                HandlersFromPreviousWindowHaveFinishedCheck handlersFromPreviousWindowHaveFinishedCheck,
                                DecrementRunningHandlerCountFun decrementRunningHandlerCountFun) throws OperationHandlerExecutorException {
        try {
            handler.addBeforeExecuteCheck(handlersFromPreviousWindowHaveFinishedCheck);
            handler.addOnCompleteTask(decrementRunningHandlerCountFun);
            operationHandlerExecutor.execute(handler);
        } catch (OperationHandlerExecutorException e) {
            throw new OperationHandlerExecutorException(
                    String.format("Error encountered while submitting operation for execution\nOperation: %s", handler.operation(), e));
        }
    }

    private class HandlersFromPreviousWindowHaveFinishedCheck implements SpinnerCheck {
        private final AtomicInteger numberOfHandlersFromPreviousWindowThatAreStillRunning;
        private final ConcurrentErrorReporter errorReporter;
        private final UniformWindowedOperationStreamExecutorServiceThread parent;

        private HandlersFromPreviousWindowHaveFinishedCheck(AtomicInteger numberOfHandlersFromPreviousWindowThatAreStillRunning,
                                                            ConcurrentErrorReporter errorReporter,
                                                            UniformWindowedOperationStreamExecutorServiceThread parent) {
            this.numberOfHandlersFromPreviousWindowThatAreStillRunning = numberOfHandlersFromPreviousWindowThatAreStillRunning;
            this.errorReporter = errorReporter;
            this.parent = parent;
        }

        @Override
        public boolean doCheck() {
            return 0 == numberOfHandlersFromPreviousWindowThatAreStillRunning.get();
        }

        @Override
        public boolean handleFailedCheck(Operation<?> operation) {
            errorReporter.reportError(
                    parent,
                    String.format("One or more handlers from the previous window did not complete in time to process operation\n%s", operation));
            return false;
        }
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
