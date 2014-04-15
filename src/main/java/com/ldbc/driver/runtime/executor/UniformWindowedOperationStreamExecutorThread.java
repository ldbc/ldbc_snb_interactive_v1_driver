package com.ldbc.driver.runtime.executor;

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
import com.ldbc.driver.runtime.scheduling.UniformWindowedScheduler;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

class UniformWindowedOperationStreamExecutorThread extends Thread {
    private final OperationHandlerExecutor operationHandlerExecutor;
    private final Scheduler<List<OperationHandler<?>>, Window.OperationHandlerTimeRangeWindow> scheduler;
    private final Spinner slightlyEarlySpinner;
    private final ConcurrentErrorReporter errorReporter;
    private final AtomicBoolean hasFinished;
    private final WindowGenerator<OperationHandler<?>, Window.OperationHandlerTimeRangeWindow> handlerWindows;

    public UniformWindowedOperationStreamExecutorThread(final Time firstWindowStartTime,
                                                        final Duration windowSize,
                                                        OperationHandlerExecutor operationHandlerExecutor,
                                                        ConcurrentErrorReporter errorReporter,
                                                        Iterator<OperationHandler<?>> handlers,
                                                        AtomicBoolean hasFinished,
                                                        Spinner slightlyEarlySpinner) {
        super(UniformWindowedOperationStreamExecutorThread.class.getSimpleName());
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
        this.handlerWindows =
                new WindowGenerator<OperationHandler<?>, Window.OperationHandlerTimeRangeWindow>(handlers, windows, WindowGenerator.PartialWindowStrategy.RETURN);
    }

    @Override
    public void run() {
        List<Future<OperationResult>> currentlyExecutingHandlers = new ArrayList<Future<OperationResult>>();
        Window.OperationHandlerTimeRangeWindow window = null;
        while (handlerWindows.hasNext()) {
            window = handlerWindows.next();
            List<OperationHandler<?>> scheduledHandlers = scheduler.schedule(window);

            // perform check at least once, in case time is ready (and within acceptable delay) on first loop
            boolean previousWindowStillExecuting = false == previousWindowCompletedExecuting(currentlyExecutingHandlers);

            // TODO why is it necessary to perform these checks here?
            // TODO - scheduler ensures operation start times can only be within window (though no runtime checks for that!!)
            // TODO - using SpinnerCheck would make sure operations don't start before previous window finishes
            // TODO - using SpinnerCheck would make sure previous operations finished in time
            // TODO
            // TODO however, waiting for start time of window is not a bad thing, as there is no way an operation should start before that
            // wait for window start time
            while (Time.nowAsMilli() < window.windowStartTimeInclusive().asMilli()) {
                // Ensure all operations from previous window have completed executing
                if (previousWindowStillExecuting && previousWindowCompletedExecuting(currentlyExecutingHandlers))
                    previousWindowStillExecuting = false;
            }

            // TODO alternative approach would be to add spinner checks to handlers, which would check that previous window handlers had completed executing
            // TODO seems cleaner and avoids need for waiting for start time of window

            if (previousWindowStillExecuting) {
                errorReporter.reportError(this, "One or more local operations in window did not complete in time");
            }

            // execute operation handlers for current window
            currentlyExecutingHandlers = new ArrayList<Future<OperationResult>>();
            for (OperationHandler<?> handler : scheduledHandlers) {
                // Schedule slightly early to account for context switch - internally, handler will schedule at exact start time
                slightlyEarlySpinner.waitForScheduledStartTime(handler.operation());
                try {
                    handler.completionTimeService().submitInitiatedTime(handler.operation().scheduledStartTime());
                } catch (CompletionTimeException e) {
                    errorReporter.reportError(this,
                            String.format("Error encountered while submitted Initiated Time for:\n\t%s\n%s",
                                    handler.operation().toString(),
                                    ConcurrentErrorReporter.stackTraceToString(e.getCause())));
                }
                try {
                    currentlyExecutingHandlers.add(operationHandlerExecutor.execute(handler));
                } catch (OperationHandlerExecutorException e) {
                    errorReporter.reportError(this,
                            String.format("Error encountered while submitting operation for execution\n\t%s\n\t%s",
                                    handler.operation().toString(),
                                    ConcurrentErrorReporter.stackTraceToString(e.getCause())));
                }
            }

        }
        // TODO use similar logic to make it possible to cap maximum query run time
        while (null != window && Time.nowAsMilli() < window.windowEndTimeExclusive().asMilli()) {
            // wait for last window to finish
        }
        this.hasFinished.set(true);
    }

    private boolean previousWindowCompletedExecuting(List<Future<OperationResult>> executingHandlers) {
        for (Future<OperationResult> resultFuture : executingHandlers)
            if (false == resultFuture.isDone()) return false;
        return true;
    }
}
