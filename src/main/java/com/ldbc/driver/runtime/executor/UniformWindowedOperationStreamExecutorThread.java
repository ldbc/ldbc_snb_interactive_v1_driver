package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
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

class UniformWindowedOperationStreamExecutorThread extends Thread {
    // TODO this value should be configurable, or an entirely better policy should be used
    private static final Duration DURATION_TO_WAIT_FOR_LAST_HANDLER_TO_FINISH = Duration.fromMinutes(30);

    private final TimeSource TIME_SOURCE;
    private final OperationHandlerExecutor operationHandlerExecutor;
    private final Scheduler<List<OperationHandler<?>>, Window.OperationHandlerTimeRangeWindow> scheduler;
    private final Spinner slightlyEarlySpinner;
    private final ConcurrentErrorReporter errorReporter;
    private final AtomicBoolean hasFinished;
    private final WindowGenerator<OperationHandler<?>, Window.OperationHandlerTimeRangeWindow> handlerWindows;

    public UniformWindowedOperationStreamExecutorThread(TimeSource timeSource,
                                                        final Time firstWindowStartTime,
                                                        final Duration windowSize,
                                                        OperationHandlerExecutor operationHandlerExecutor,
                                                        ConcurrentErrorReporter errorReporter,
                                                        Iterator<OperationHandler<?>> handlers,
                                                        AtomicBoolean hasFinished,
                                                        Spinner slightlyEarlySpinner) {
        super(UniformWindowedOperationStreamExecutorThread.class.getSimpleName());
        this.TIME_SOURCE = timeSource;
        this.operationHandlerExecutor = operationHandlerExecutor;
        this.scheduler = new UniformWindowedScheduler();
        this.slightlyEarlySpinner = slightlyEarlySpinner;
        this.errorReporter = errorReporter;
        this.hasFinished = hasFinished;
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
        List<Future<OperationResult>> executingHandlersFromCurrentWindow = new ArrayList<>();
        List<Future<OperationResult>> executingHandlersFromPreviousWindow = executingHandlersFromCurrentWindow;
        HandlersFromPreviousWindowHaveFinishedCheck handlersFromPreviousWindowHaveFinishedCheck =
                new HandlersFromPreviousWindowHaveFinishedCheck(
                        executingHandlersFromPreviousWindow,
                        errorReporter,
                        this);
        while (handlerWindows.hasNext()) {
            executingHandlersFromPreviousWindow = executingHandlersFromCurrentWindow;
            executingHandlersFromCurrentWindow = new ArrayList<>();
            handlersFromPreviousWindowHaveFinishedCheck =
                    new HandlersFromPreviousWindowHaveFinishedCheck(
                            executingHandlersFromPreviousWindow,
                            errorReporter,
                            this);
            window = handlerWindows.next();
            List<OperationHandler<?>> scheduledHandlers = scheduler.schedule(window);

            // execute operation handlers for current window
            for (OperationHandler<?> handler : scheduledHandlers) {
                // Schedule slightly early to account for context switch - internally, handler will schedule at exact start time
                slightlyEarlySpinner.waitForScheduledStartTime(handler.operation());

                try {
                    handler.completionTimeService().submitInitiatedTime(handler.operation().scheduledStartTime());
                } catch (CompletionTimeException e) {
                    errorReporter.reportError(this,
                            String.format("Error encountered while submitted Initiated Time for:\n\t%s\n%s",
                                    handler.operation().toString(),
                                    ConcurrentErrorReporter.stackTraceToString(e)));
                }
                try {
                    handler.addCheck(handlersFromPreviousWindowHaveFinishedCheck);
                    Future<OperationResult> executingHandler = operationHandlerExecutor.execute(handler);
                    executingHandlersFromCurrentWindow.add(executingHandler);
                } catch (OperationHandlerExecutorException e) {
                    errorReporter.reportError(this,
                            String.format("Error encountered while submitting operation for execution\n\t%s\n\t%s",
                                    handler.operation().toString(),
                                    ConcurrentErrorReporter.stackTraceToString(e)));
                }
            }

        }

        if (null != window) {
            // long waitUntilTimeAsMilli = TIME_SOURCE.now().plus(DURATION_TO_WAIT_FOR_LAST_HANDLER_TO_FINISH).asMilli();
            long latestTimeToWaitAsMilli = window.windowEndTimeExclusive().asMilli();
            // wait for operations from last window to finish executing
            while (false == handlersFromPreviousWindowHaveFinishedCheck.doCheck()) {
                if (TIME_SOURCE.nowAsMilli() > latestTimeToWaitAsMilli) {
                    errorReporter.reportError(this, "One or more handlers from the last window took too long to complete");
                    break;
                }
            }
        }

        this.hasFinished.set(true);
    }

    private class HandlersFromPreviousWindowHaveFinishedCheck implements SpinnerCheck {
        private final List<Future<OperationResult>> futuresForHandlersFromPreviousWindow;
        private final ConcurrentErrorReporter errorReporter;
        private final UniformWindowedOperationStreamExecutorThread parent;
        private boolean checkResult;

        HandlersFromPreviousWindowHaveFinishedCheck(
                List<Future<OperationResult>> futuresForHandlersFromPreviousWindow,
                ConcurrentErrorReporter errorReporter,
                UniformWindowedOperationStreamExecutorThread parent) {
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
