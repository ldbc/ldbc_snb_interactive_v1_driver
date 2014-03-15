package com.ldbc.driver.runtime.executor_NEW;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.generator.Window;
import com.ldbc.driver.generator.WindowGenerator;
import com.ldbc.driver.runtime.error.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.error.ExecutionDelayPolicy;
import com.ldbc.driver.runtime.error.LoggingExecutionDelayPolicy;
import com.ldbc.driver.runtime.executor.Spinner;
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
    // TODO this is an arbitrary value, should be configurable
    public static final Duration DEFAULT_TOLERATED_OPERATION_START_TIME_DELAY = Duration.fromSeconds(1);
    // TODO this needs to be tuned perhaps a method that during initialization runs an ExecutionService and measures delay
    private final Duration SPINNER_OFFSET_DURATION = Duration.fromMilli(100);

    private final Generator<Window.OperationHandlerTimeRangeWindow> windows;
    private final ThreadPoolOperationHandlerExecutor operationHandlerExecutor;
    private final Scheduler<List<OperationHandler<?>>, Window.OperationHandlerTimeRangeWindow> scheduler;
    private final Spinner slightlyEarlySpinner;
    private final ConcurrentErrorReporter concurrentErrorReporter;
    private final ConcurrentCompletionTimeService concurrentCompletionTimeService;
    private final Iterator<OperationHandler<?>> handlers;
    private final AtomicBoolean hasFinished;

    public UniformWindowedOperationStreamExecutorThread(final Time startTime,
                                                        final Duration windowSize,
                                                        int threadCount,
                                                        ConcurrentErrorReporter concurrentErrorReporter,
                                                        ConcurrentCompletionTimeService concurrentCompletionTimeService,
                                                        Iterator<OperationHandler<?>> handlers,
                                                        AtomicBoolean hasFinished) {
        this.windows = new Generator<Window.OperationHandlerTimeRangeWindow>() {
            private Time windowStartTime = startTime;

            @Override
            protected Window.OperationHandlerTimeRangeWindow doNext() throws GeneratorException {
                Window.OperationHandlerTimeRangeWindow window = new Window.OperationHandlerTimeRangeWindow(windowStartTime, windowSize);
                windowStartTime = windowStartTime.plus(windowSize);
                return window;
            }
        };
        this.operationHandlerExecutor = new ThreadPoolOperationHandlerExecutor(threadCount);
        this.scheduler = new UniformWindowedScheduler();
        // TODO make ExecutionDelayPolicy configurable
        // TODO make allowable delay configurable
        // TODO make ignore start time configurable
        ExecutionDelayPolicy executionDelayPolicy = new LoggingExecutionDelayPolicy(
                DEFAULT_TOLERATED_OPERATION_START_TIME_DELAY);
        this.slightlyEarlySpinner = new Spinner(executionDelayPolicy, SPINNER_OFFSET_DURATION);
        this.concurrentErrorReporter = concurrentErrorReporter;
        this.concurrentCompletionTimeService = concurrentCompletionTimeService;
        this.handlers = handlers;
        this.hasFinished = hasFinished;
    }

    @Override
    public void run() {
        // removes windows of handlers, where every handler in each window is within the same time range/window
        WindowGenerator<OperationHandler<?>, Window.OperationHandlerTimeRangeWindow> handlerWindows =
                new WindowGenerator<OperationHandler<?>, Window.OperationHandlerTimeRangeWindow>(
                        handlers,
                        windows,
                        WindowGenerator.PartialWindowStrategy.RETURN);

        // retrieve and execute windows of handlers, one window at a time
        List<Future<OperationResult>> currentlyExecutingHandlers = new ArrayList<Future<OperationResult>>();
        Time endTimeOfLastWindow = Time.now().minus(Duration.fromMilli(1));
        while (handlerWindows.hasNext()) {
            Window.OperationHandlerTimeRangeWindow window = handlerWindows.next();
            List<OperationHandler<?>> scheduledHandlers = scheduler.schedule(window);
            // wait for end of previous time window
            // TODO use something like Spinner here?
            while (Time.nowAsMilli() < endTimeOfLastWindow.asMilli()) {
                // loop/wait until window time complete
            }
            if (false == allHandlersHaveCompleted(currentlyExecutingHandlers)) {
                // TODO replace report & return with Policy, which may log or report or whatever
                // TODO use Spinner?
                concurrentErrorReporter.reportError(this, "All operations in window did not complete in time");
                return;
            }
            currentlyExecutingHandlers = new ArrayList<Future<OperationResult>>();
            for (OperationHandler<?> operationHandler : scheduledHandlers) {
                // Schedule slightly early to account for context switch latency
                // Internally, OperationHandler will schedule at exact scheduled start time
                slightlyEarlySpinner.waitForScheduledStartTime(operationHandler.operation());
                try {
                    concurrentCompletionTimeService.submitInitiatedTime(operationHandler.operation().scheduledStartTime());
                } catch (CompletionTimeException e) {
                    String errMsg = String.format("Unexpected error encountered while submitted Initiated Time for:\n\t%s\n%s",
                            operationHandler.operation().toString(),
                            ConcurrentErrorReporter.stackTraceToString(e.getCause()));
                    concurrentErrorReporter.reportError(this, errMsg);
                }
                Future<OperationResult> handlerFuture = operationHandlerExecutor.execute(operationHandler);
                currentlyExecutingHandlers.add(handlerFuture);
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
