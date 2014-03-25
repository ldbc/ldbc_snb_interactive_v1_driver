package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.generator.Window;
import com.ldbc.driver.generator.WindowGenerator;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.error.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.scheduler.Scheduler;
import com.ldbc.driver.runtime.scheduler.UniformWindowedScheduler;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

class UniformWindowedOperationStreamExecutorThread extends Thread {
    private final CompletionTimeValidator completionTimeValidator;
    private final OperationHandlerExecutor operationHandlerExecutor;
    private final Scheduler<List<OperationHandler<?>>, Window.OperationHandlerTimeRangeWindow> scheduler;
    private final AlwaysValidCompletionTimeValidator.Spinner slightlyEarlySpinner;
    private final ConcurrentErrorReporter errorReporter;
    private final ConcurrentCompletionTimeService completionTimeService;
    private final AtomicBoolean hasFinished;
    private final WindowGenerator<OperationHandler<?>, Window.OperationHandlerTimeRangeWindow> handlerWindows;

    // TODO take Scheduler as input parameter rather than creating here, then same Thread class could be used by different Service classes
    public UniformWindowedOperationStreamExecutorThread(final Time firstWindowStartTime,
                                                        final Duration windowSize,
                                                        CompletionTimeValidator completionTimeValidator,
                                                        OperationHandlerExecutor operationHandlerExecutor,
                                                        ConcurrentErrorReporter errorReporter,
                                                        ConcurrentCompletionTimeService completionTimeService,
                                                        Iterator<OperationHandler<?>> handlers,
                                                        AtomicBoolean hasFinished,
                                                        AlwaysValidCompletionTimeValidator.Spinner slightlyEarlySpinner) {
        this.completionTimeValidator = completionTimeValidator;
        this.operationHandlerExecutor = operationHandlerExecutor;
        this.scheduler = new UniformWindowedScheduler();
        this.slightlyEarlySpinner = slightlyEarlySpinner;
        this.errorReporter = errorReporter;
        this.completionTimeService = completionTimeService;
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
        this.handlerWindows = new WindowGenerator<OperationHandler<?>, Window.OperationHandlerTimeRangeWindow>(
                handlers,
                windows,
                WindowGenerator.PartialWindowStrategy.RETURN);
    }

    @Override
    public void run() {
        // retrieve and execute windows of handlers, one window at a time
        List<Future<OperationResult>> currentlyExecutingHandlers = new ArrayList<Future<OperationResult>>();
        Time endTimeOfLastWindow = Time.now().minus(Duration.fromMilli(1));
        while (handlerWindows.hasNext()) {
            Window.OperationHandlerTimeRangeWindow window = handlerWindows.next();
            List<OperationHandler<?>> scheduledHandlers = scheduler.schedule(window);

            // wait for end of previous time window
            // TODO use something like Spinner here? OperationSpinner uses TimeSpinner?
            while (Time.nowAsMilli() < endTimeOfLastWindow.asMilli()) {
                // loop/wait until window time complete
            }

            // Ensure all previous window operations have completed
            if (false == allHandlersHaveCompleted(currentlyExecutingHandlers)) {
                errorReporter.reportError(this, "One or more local operations in window did not complete in time");
            }

            // Ensure GCT has proceeded enough to execute next window
            try {
                if (false == completionTimeValidator.isValid(completionTimeService, window.windowStartTimeInclusive())) {
                    errorReporter.reportError(
                            this,
                            String.format("GCT advanced too slowly\nCurrent window [%s,%s) < GCT(%s) + DeltaT",
                                    completionTimeService.globalCompletionTime().toString(),
                                    window.windowStartTimeInclusive().toString(),
                                    window.windowEndTimeExclusive().toString()));
                }
            } catch (CompletionTimeException e) {
                errorReporter.reportError(
                        this,
                        String.format("Error encountered while reading GCT\n%s",
                                ConcurrentErrorReporter.stackTraceToString(e.getCause())));
            }

            // execute operation handlers for current window
            currentlyExecutingHandlers = new ArrayList<Future<OperationResult>>();
            for (OperationHandler<?> operationHandler : scheduledHandlers) {
                // Schedule slightly early to account for context switch latency
                // Internally, OperationHandler will schedule at exact scheduled start time
                slightlyEarlySpinner.waitForScheduledStartTime(operationHandler.operation());
                try {
                    completionTimeService.submitInitiatedTime(operationHandler.operation().scheduledStartTime());
                } catch (CompletionTimeException e) {
                    String errMsg = String.format("Error encountered while submitted Initiated Time for:\n\t%s\n%s",
                            operationHandler.operation().toString(),
                            ConcurrentErrorReporter.stackTraceToString(e.getCause()));
                    errorReporter.reportError(this, errMsg);
                }
                try {
                    currentlyExecutingHandlers.add(operationHandlerExecutor.execute(operationHandler));
                } catch (OperationHandlerExecutorException e) {
                    String errMsg = String.format("Error encountered while submitting operation for execution\n\t%s\n\t%s",
                            operationHandler.operation().toString(),
                            ConcurrentErrorReporter.stackTraceToString(e.getCause()));
                    errorReporter.reportError(this, errMsg);
                }
            }
            endTimeOfLastWindow = window.windowEndTimeExclusive();
        }
        this.hasFinished.set(true);
    }

    private boolean allHandlersHaveCompleted(List<Future<OperationResult>> executingHandlers) {
        for (Future<OperationResult> resultFuture : executingHandlers) {
            if (false == resultFuture.isDone()) {
                return false;
            }
        }
        return true;
    }
}
