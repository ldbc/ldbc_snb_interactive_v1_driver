package com.ldbc.driver.runtime;

import com.ldbc.driver.*;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.executor.*;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.ErrorReportingTerminatingExecutionDelayPolicy;
import com.ldbc.driver.runtime.scheduling.ExecutionDelayPolicy;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.runtime.streams.IteratorSplitter;
import com.ldbc.driver.runtime.streams.IteratorSplittingException;
import com.ldbc.driver.runtime.streams.SplitDefinition;
import com.ldbc.driver.runtime.streams.SplitResult;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ldbc.driver.OperationClassification.SchedulingMode;

public class WorkloadRunner {
    public static final Duration EARLY_SPINNER_OFFSET_DURATION = Duration.fromMilli(100);
    private static final Duration WAIT_DURATION_FOR_OPERATION_HANDLER_EXECUTOR_TO_SHUTDOWN = Duration.fromSeconds(5);
    public static final long RUNNER_POLLING_INTERVAL_AS_MILLI = Duration.fromMilli(500).asMilli();

    private final TimeSource TIME_SOURCE;

    private final Spinner exactSpinner;
    private final Spinner earlySpinner;

    // TODO make service and inject into workload runner. this could report to coordinator OR a local console printer, for example
    private WorkloadStatusThread workloadStatusThread;

    private final ConcurrentErrorReporter errorReporter;

    private final OperationHandlerExecutor operationHandlerExecutor;

    private final PreciseIndividualAsyncOperationStreamExecutorService preciseIndividualAsyncOperationStreamExecutorService;
    private final PreciseIndividualBlockingOperationStreamExecutorService preciseIndividualBlockingOperationStreamExecutorService;
    private final UniformWindowedOperationStreamExecutorService uniformWindowedOperationStreamExecutorService;

    private final Duration statusDisplayInterval;

    public WorkloadRunner(TimeSource timeSource,
                          Db db,
                          Iterator<Operation<?>> operations,
                          Map<Class<? extends Operation>, OperationClassification> operationClassifications,
                          ConcurrentMetricsService metricsService,
                          ConcurrentErrorReporter errorReporter,
                          ConcurrentCompletionTimeService completionTimeService,
                          int threadCount,
                          Duration statusDisplayInterval,
                          Time workloadStartTime,
                          Duration toleratedExecutionDelayDuration,
                          Duration spinnerSleepDuration,
                          Duration executionWindowDuration,
                          Duration earlySpinnerOffsetDuration) throws WorkloadException {
        this.TIME_SOURCE = timeSource;
        this.errorReporter = errorReporter;
        this.statusDisplayInterval = statusDisplayInterval;

        ExecutionDelayPolicy executionDelayPolicy = new ErrorReportingTerminatingExecutionDelayPolicy(
                TIME_SOURCE,
                toleratedExecutionDelayDuration,
                errorReporter);

        // TODO for the spinner sent to Window scheduler allow delay to reach to the end of window?

        this.exactSpinner = new Spinner(TIME_SOURCE, spinnerSleepDuration, executionDelayPolicy);
        this.earlySpinner = new Spinner(TIME_SOURCE, spinnerSleepDuration, executionDelayPolicy, earlySpinnerOffsetDuration);
        // TODO make this a configuration parameter?
        boolean detailedStatus = true;
        if (statusDisplayInterval.asSeconds() > 0)
            this.workloadStatusThread = new WorkloadStatusThread(
                    statusDisplayInterval,
                    metricsService,
                    errorReporter,
                    completionTimeService,
                    detailedStatus);
        List<Operation<?>> windowedOperations;
        List<Operation<?>> blockingOperations;
        List<Operation<?>> asynchronousOperations;
        try {
            IteratorSplitter<Operation<?>> splitter = new IteratorSplitter<>(IteratorSplitter.UnmappedItemPolicy.ABORT);
            SplitDefinition<Operation<?>> windowed = new SplitDefinition<>(Workload.operationTypesBySchedulingMode(operationClassifications, SchedulingMode.WINDOWED));
            SplitDefinition<Operation<?>> blocking = new SplitDefinition<>(Workload.operationTypesBySchedulingMode(operationClassifications, SchedulingMode.INDIVIDUAL_BLOCKING));
            SplitDefinition<Operation<?>> asynchronous = new SplitDefinition<>(Workload.operationTypesBySchedulingMode(operationClassifications, SchedulingMode.INDIVIDUAL_ASYNC));
            SplitResult splits = splitter.split(operations, windowed, blocking, asynchronous);
            windowedOperations = splits.getSplitFor(windowed);
            blockingOperations = splits.getSplitFor(blocking);
            asynchronousOperations = splits.getSplitFor(asynchronous);
        } catch (IteratorSplittingException e) {
            throw new WorkloadException(
                    String.format("Error while splitting operation stream by scheduling mode\n%s", ConcurrentErrorReporter.stackTraceToString(e)),
                    e);
        }

        OperationsToHandlersTransformer operationsToHandlers = new OperationsToHandlersTransformer(
                TIME_SOURCE,
                db,
                exactSpinner,
                completionTimeService,
                errorReporter,
                metricsService,
                operationClassifications);

        Iterator<OperationHandler<?>> windowedHandlers = operationsToHandlers.transform(windowedOperations.iterator());
        Iterator<OperationHandler<?>> blockingHandlers = operationsToHandlers.transform(blockingOperations.iterator());
        Iterator<OperationHandler<?>> asynchronousHandlers = operationsToHandlers.transform(asynchronousOperations.iterator());

        // TODO (past alex) these executor services should all be using different gct services and sharing gct via external ct [MUST]
        // TODO (past alex) This lesson needs to be written to Confluence too
        // TODO (present alex) why?
        // TODO (present alex) is it because, if one scheduler is lagging, GCT may advance before a slower executor
        // TODO (present alex) submits an initiated time for an operation who's scheduled start time is already behind GCT?
        // TODO (present alex) if operations are close together (closer than tolerated delay) it seems like this is definitely possible

        // TODO really need to give executors more control over thread pools, or ideally their own thread pools
        this.operationHandlerExecutor = new ThreadPoolOperationHandlerExecutor(threadCount);
        this.preciseIndividualAsyncOperationStreamExecutorService = new PreciseIndividualAsyncOperationStreamExecutorService(
                TIME_SOURCE, errorReporter, asynchronousHandlers, earlySpinner, operationHandlerExecutor);
        this.preciseIndividualBlockingOperationStreamExecutorService = new PreciseIndividualBlockingOperationStreamExecutorService(
                TIME_SOURCE, errorReporter, blockingHandlers, earlySpinner, operationHandlerExecutor);
        this.uniformWindowedOperationStreamExecutorService = new UniformWindowedOperationStreamExecutorService(
                TIME_SOURCE, errorReporter, windowedHandlers, operationHandlerExecutor, earlySpinner, workloadStartTime, executionWindowDuration);
    }

    public void executeWorkload() throws WorkloadException {
        if (statusDisplayInterval.asSeconds() > 0)
            workloadStatusThread.start();

        AtomicBoolean asyncHandlersFinished = preciseIndividualAsyncOperationStreamExecutorService.execute();
        AtomicBoolean blockingHandlersFinished = preciseIndividualBlockingOperationStreamExecutorService.execute();
        AtomicBoolean windowedHandlersFinished = uniformWindowedOperationStreamExecutorService.execute();

        while (true) {
            // Error encountered in one or more of the worker threads --> terminate run
            if (errorReporter.errorEncountered()) {
                boolean forced = true;
                String shutdownErrMsg = shutdownEverything(forced);
                throw new WorkloadException(String.format("%s\nError encountered while running workload\n%s",
                        shutdownErrMsg,
                        errorReporter.toString()));
            }

            // All executors have completed --> return
            if (asyncHandlersFinished.get() && blockingHandlersFinished.get() && windowedHandlersFinished.get())
                break;

            // Take short break between error & completion checks to reduce CPU utilization
            Spinner.powerNap(RUNNER_POLLING_INTERVAL_AS_MILLI);
        }

        // One last check for errors encountered in any of the worker threads --> terminate run
        if (errorReporter.errorEncountered()) {
            boolean forced = true;
            String showdownErrMsg = shutdownEverything(forced);
            throw new WorkloadException(String.format("%sEncountered error while running workload. Driver terminating.\n%s",
                    showdownErrMsg,
                    errorReporter.toString()));
        }

        boolean forced = false;
        String shutdownErrMsg = shutdownEverything(forced);

        if (false == "".equals(shutdownErrMsg)) {
            throw new WorkloadException(shutdownErrMsg);
        }
    }

    private String shutdownEverything(boolean forced) {
        String errMsg = "";

        try {
            preciseIndividualAsyncOperationStreamExecutorService.shutdown();
        } catch (OperationHandlerExecutorException e) {
            errMsg += String.format("Encountered error while shutting down %s\n%s\n",
                    preciseIndividualAsyncOperationStreamExecutorService.getClass().getSimpleName(),
                    ConcurrentErrorReporter.stackTraceToString(e));
        }

        try {
            preciseIndividualBlockingOperationStreamExecutorService.shutdown();
        } catch (OperationHandlerExecutorException e) {
            errMsg += String.format("Encountered error while shutting down %s\n%s\n",
                    preciseIndividualBlockingOperationStreamExecutorService.getClass().getSimpleName(),
                    ConcurrentErrorReporter.stackTraceToString(e));
        }

        try {
            uniformWindowedOperationStreamExecutorService.shutdown();
        } catch (OperationHandlerExecutorException e) {
            errMsg += String.format("Encountered error while shutting down %s\n%s\n",
                    uniformWindowedOperationStreamExecutorService.getClass().getSimpleName(),
                    ConcurrentErrorReporter.stackTraceToString(e));
        }

        try {
            if (forced)
                // if forced shutdown (error) some handlers likely still running, but it does not matter
                operationHandlerExecutor.shutdown(Duration.fromMilli(0));
            else
                // if normal shutdown all executors have completed by this stage
                operationHandlerExecutor.shutdown(WAIT_DURATION_FOR_OPERATION_HANDLER_EXECUTOR_TO_SHUTDOWN);
        } catch (OperationHandlerExecutorException e) {
            errMsg += String.format("Encountered error while shutting down %s\n%s\n",
                    operationHandlerExecutor.getClass().getSimpleName(),
                    ConcurrentErrorReporter.stackTraceToString(e));
        }

        if (statusDisplayInterval.asSeconds() > 0) {
            workloadStatusThread.shutdown();
            workloadStatusThread.interrupt();
        }

        return errMsg;
    }
}
