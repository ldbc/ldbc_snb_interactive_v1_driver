package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.generator.Window;
import com.ldbc.driver.generator.WindowGenerator;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.scheduling.Scheduler;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.runtime.scheduling.SpinnerCheck;
import com.ldbc.driver.runtime.scheduling.UniformWindowedScheduler;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

class UniformWindowedOperationStreamExecutorServiceThread extends Thread {
    // TODO this value should be configurable, or an entirely better policy should be used
    private static final Duration DURATION_TO_WAIT_FOR_LAST_HANDLER_TO_FINISH = Duration.fromMinutes(30);
    private static final Duration POLL_INTERVAL_WHILE_WAITING_FOR_LAST_HANDLER_TO_FINISH = Duration.fromMilli(100);

    private final TimeSource TIME_SOURCE;
    private final OperationHandlerExecutor operationHandlerExecutor;
    private final Scheduler<List<OperationHandler<?>>, Window.OperationHandlerTimeRangeWindow> scheduler;
    private final Spinner slightlyEarlySpinner;
    private final ConcurrentErrorReporter errorReporter;
    private final AtomicBoolean hasFinished;
    private final WindowGenerator<OperationHandler<?>, Window.OperationHandlerTimeRangeWindow> handlerWindows;
    private final AtomicBoolean forcedTerminate;

    public UniformWindowedOperationStreamExecutorServiceThread(TimeSource timeSource,
                                                               final Time firstWindowStartTime,
                                                               final Duration windowSize,
                                                               OperationHandlerExecutor operationHandlerExecutor,
                                                               ConcurrentErrorReporter errorReporter,
                                                               Iterator<OperationHandler<?>> handlers,
                                                               AtomicBoolean hasFinished,
                                                               Spinner slightlyEarlySpinner,
                                                               AtomicBoolean forcedTerminate) {
        super(UniformWindowedOperationStreamExecutorServiceThread.class.getSimpleName() + System.currentTimeMillis());
        this.TIME_SOURCE = timeSource;
        this.operationHandlerExecutor = operationHandlerExecutor;
        this.scheduler = new UniformWindowedScheduler();
        this.slightlyEarlySpinner = slightlyEarlySpinner;
        this.errorReporter = errorReporter;
        this.hasFinished = hasFinished;
        this.forcedTerminate = forcedTerminate;
        // generates windows with appropriate start and end times
        Generator<Window.OperationHandlerTimeRangeWindow> windows = new Generator<Window.OperationHandlerTimeRangeWindow>() {
            private Time windowStartTime = firstWindowStartTime;

            @Override
            protected Window.OperationHandlerTimeRangeWindow doNext() throws GeneratorException {
                Window.OperationHandlerTimeRangeWindow window = new Window.OperationHandlerTimeRangeWindow(windowStartTime, windowSize);
                windowStartTime = windowStartTime.plus(windowSize);
                return window;
            }
        };
        // removes windows of handlers, where every handler in each window is within the same time range/window
        this.handlerWindows = new WindowGenerator<>(handlers, windows, WindowGenerator.PartialWindowStrategy.RETURN);
    }

    @Override
    public void run() {
        Window.OperationHandlerTimeRangeWindow window = null;
        List<Future<OperationResultReport>> executingHandlersFromCurrentWindow = new ArrayList<>();
        List<Future<OperationResultReport>> executingHandlersFromPreviousWindow = new ArrayList<>();
        HandlersFromPreviousWindowHaveFinishedCheck handlersFromPreviousWindowHaveFinishedCheck =
                new HandlersFromPreviousWindowHaveFinishedCheck(
                        executingHandlersFromPreviousWindow,
                        errorReporter,
                        this);
        while (handlerWindows.hasNext() && false == forcedTerminate.get()) {
            executingHandlersFromPreviousWindow = executingHandlersFromCurrentWindow;
            executingHandlersFromCurrentWindow = new ArrayList<>();
            handlersFromPreviousWindowHaveFinishedCheck =
                    new HandlersFromPreviousWindowHaveFinishedCheck(
                            executingHandlersFromPreviousWindow,
                            errorReporter,
                            this);
            window = handlerWindows.next();
            List<OperationHandler<?>> scheduledHandlers = scheduler.schedule(window);

            // submit initiated time as soon as possible so GCT/dependencies can advance as soon as possible
            for (int i = 0; i < scheduledHandlers.size(); i++) {
                OperationHandler<?> handler = scheduledHandlers.get(i);
                submitInitiatedTime(handler);
            }

            // execute operation handlers for current window
            for (int i = 0; i < scheduledHandlers.size(); i++) {
                OperationHandler<?> handler = scheduledHandlers.get(i);
                // Schedule slightly early to account for context switch - internally, handler will schedule at exact start time
                // TODO forcedTerminate does not cover all cases at present this spin loop is still blocking -> inject a check that throws exception?
                // TODO or SpinnerChecks have three possible results? (TRUE, NOT_TRUE_YET, FALSE)
                // TODO and/or Spinner has an emergency terminate button?
                slightlyEarlySpinner.waitForScheduledStartTime(handler.operation());

                executeHandler(
                        handler,
                        handlersFromPreviousWindowHaveFinishedCheck,
                        executingHandlersFromCurrentWindow);
            }
        }

        if (null != window) {
            long pollInterval = POLL_INTERVAL_WHILE_WAITING_FOR_LAST_HANDLER_TO_FINISH.asMilli();
            // long waitUntilTimeAsMilli = TIME_SOURCE.now().plus(DURATION_TO_WAIT_FOR_LAST_HANDLER_TO_FINISH).asMilli();
            long latestTimeToWaitAsMilli = window.windowEndTimeExclusive().asMilli();
            // wait for operations from last window to finish executing
            while (false == handlersFromPreviousWindowHaveFinishedCheck.doCheck()) {
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

    private void submitInitiatedTime(OperationHandler<?> handler) {
        try {
            handler.localCompletionTimeWriter().submitLocalInitiatedTime(handler.operation().scheduledStartTime());
        } catch (CompletionTimeException e) {
            errorReporter.reportError(this,
                    String.format("Error encountered while submitted Initiated Time for:\n\t%s\n%s",
                            handler.operation().toString(),
                            ConcurrentErrorReporter.stackTraceToString(e)));
        }
    }

    private void executeHandler(OperationHandler<?> handler,
                                HandlersFromPreviousWindowHaveFinishedCheck handlersFromPreviousWindowHaveFinishedCheck,
                                List<Future<OperationResultReport>> executingHandlersFromCurrentWindow) {
        try {
            handler.addCheck(handlersFromPreviousWindowHaveFinishedCheck);
            Future<OperationResultReport> executingHandler = operationHandlerExecutor.execute(handler);
            executingHandlersFromCurrentWindow.add(executingHandler);
        } catch (OperationHandlerExecutorException e) {
            errorReporter.reportError(this,
                    String.format("Error encountered while submitting operation for execution\n\t%s\n\t%s",
                            handler.operation().toString(),
                            ConcurrentErrorReporter.stackTraceToString(e)));
        }
    }

    private class HandlersFromPreviousWindowHaveFinishedCheck implements SpinnerCheck {
        private final List<Future<OperationResultReport>> futuresForHandlersFromPreviousWindow;
        private final ConcurrentErrorReporter errorReporter;
        private final UniformWindowedOperationStreamExecutorServiceThread parent;
        private boolean checkResult;

        HandlersFromPreviousWindowHaveFinishedCheck(
                List<Future<OperationResultReport>> futuresForHandlersFromPreviousWindow,
                ConcurrentErrorReporter errorReporter,
                UniformWindowedOperationStreamExecutorServiceThread parent) {
            this.futuresForHandlersFromPreviousWindow = futuresForHandlersFromPreviousWindow;
            this.errorReporter = errorReporter;
            this.parent = parent;
            this.checkResult = false;
        }

        @Override
        public boolean doCheck() {
            if (checkResult) return true;
            for (int i = 0; i < futuresForHandlersFromPreviousWindow.size(); i++) {
                if (false == futuresForHandlersFromPreviousWindow.get(i).isDone())
                    return false;
            }
            checkResult = true;
            return true;
        }

        @Override
        public boolean handleFailedCheck(Operation<?> operation) {
            errorReporter.reportError(
                    parent,
                    String.format("One or more handlers from the previous window did not complete in time to process operation\n%s", operation));
            return false;
        }
    }
}
