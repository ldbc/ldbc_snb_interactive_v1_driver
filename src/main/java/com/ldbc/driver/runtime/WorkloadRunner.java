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
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ldbc.driver.OperationClassification.SchedulingMode;

public class WorkloadRunner {
    private final TimeSource TIME_SOURCE;

    private final Duration WAIT_DURATION_FOR_OPERATION_HANDLER_EXECUTOR_TO_SHUTDOWN = Duration.fromSeconds(5);
    private final Duration DEFAULT_STATUS_UPDATE_INTERVAL = Duration.fromSeconds(2);
    private final Duration SPINNER_OFFSET_DURATION = Duration.fromMilli(100);

    private final Spinner exactSpinner;
    private final Spinner earlySpinner;

    // TODO make service and inject into workload runner. this could report to coordinator OR a local console printer, for example
    private final WorkloadStatusThread workloadStatusThread;

    private final ConcurrentErrorReporter errorReporter;

    private final OperationHandlerExecutor operationHandlerExecutor;

    private final PreciseIndividualAsyncOperationStreamExecutorService preciseIndividualAsyncOperationStreamExecutorService;
    private final PreciseIndividualBlockingOperationStreamExecutorService preciseIndividualBlockingOperationStreamExecutorService;
    private final UniformWindowedOperationStreamExecutorService uniformWindowedOperationStreamExecutorService;

    private final boolean showStatus;

    public WorkloadRunner(TimeSource timeSource,
                          Db db,
                          Iterator<Operation<?>> operations,
                          Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications,
                          ConcurrentMetricsService metricsService,
                          ConcurrentErrorReporter errorReporter,
                          ConcurrentCompletionTimeService completionTimeService,
                          int threadCount,
                          boolean showStatus,
                          Time workloadStartTime,
                          Duration toleratedExecutionDelayDuration,
                          Duration spinnerSleepDuration,
                          Duration gctDeltaDuration,
                          Duration executionWindowDuration) throws WorkloadException {
        this.TIME_SOURCE = timeSource;
        this.errorReporter = errorReporter;
        this.showStatus = showStatus;

        ExecutionDelayPolicy executionDelayPolicy = new ErrorReportingTerminatingExecutionDelayPolicy(
                TIME_SOURCE,
                toleratedExecutionDelayDuration,
                errorReporter);

        // TODO for the spinner sent to Window scheduler allow delay to reach to the end of window?

        this.exactSpinner = new Spinner(TIME_SOURCE, spinnerSleepDuration, executionDelayPolicy);
        this.earlySpinner = new Spinner(TIME_SOURCE, spinnerSleepDuration, executionDelayPolicy, SPINNER_OFFSET_DURATION);
        this.workloadStatusThread = new WorkloadStatusThread(DEFAULT_STATUS_UPDATE_INTERVAL, metricsService, errorReporter);
        Iterator<Operation<?>> windowedOperations;
        Iterator<Operation<?>> blockingOperations;
        Iterator<Operation<?>> asynchronousOperations;
        try {
            IteratorSplitter<Operation<?>> splitter = new IteratorSplitter<>(IteratorSplitter.UnmappedItemPolicy.ABORT);
            SplitDefinition<Operation<?>> windowed = new SplitDefinition<>(Workload.operationTypesBySchedulingMode(operationClassifications, SchedulingMode.WINDOWED));
            SplitDefinition<Operation<?>> blocking = new SplitDefinition<>(Workload.operationTypesBySchedulingMode(operationClassifications, SchedulingMode.INDIVIDUAL_BLOCKING));
            SplitDefinition<Operation<?>> asynchronous = new SplitDefinition<>(Workload.operationTypesBySchedulingMode(operationClassifications, SchedulingMode.INDIVIDUAL_ASYNC));
            SplitResult splits = splitter.split(operations, windowed, blocking, asynchronous);
            windowedOperations = splits.getSplitFor(windowed).iterator();
            blockingOperations = splits.getSplitFor(blocking).iterator();
            asynchronousOperations = splits.getSplitFor(asynchronous).iterator();
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
                gctDeltaDuration,
                operationClassifications);
        Iterator<OperationHandler<?>> windowedHandlers = operationsToHandlers.transform(windowedOperations);
        Iterator<OperationHandler<?>> blockingHandlers = operationsToHandlers.transform(blockingOperations);
        Iterator<OperationHandler<?>> asynchronousHandlers = operationsToHandlers.transform(asynchronousOperations);

        // TODO (past alex) these executor services should all be using different gct services and sharing gct via external ct [MUST]
        // TODO (past alex) This lesson needs to be written to Confluence too
        // TODO (present alex) why?
        // TODO (present alex) is it because, if one scheduler is lagging, GCT may advance before a slower executor
        // TODO (present alex) submits an initiated time for an operation who's scheduled start time is already behind GCT?
        // TODO (present alex) if operations are close together (closer than tolerated delay) it seems like this is definitely possible

        this.operationHandlerExecutor = new ThreadPoolOperationHandlerExecutor(threadCount);
        this.preciseIndividualAsyncOperationStreamExecutorService = new PreciseIndividualAsyncOperationStreamExecutorService(
                TIME_SOURCE, errorReporter, asynchronousHandlers, earlySpinner, operationHandlerExecutor);
        this.preciseIndividualBlockingOperationStreamExecutorService = new PreciseIndividualBlockingOperationStreamExecutorService(
                TIME_SOURCE, errorReporter, blockingHandlers, earlySpinner, operationHandlerExecutor);
        // TODO better way of setting window size. it does not need to equal DeltaT, it can be smaller. where to set? how to set?
        this.uniformWindowedOperationStreamExecutorService = new UniformWindowedOperationStreamExecutorService(
                TIME_SOURCE, errorReporter, windowedHandlers, operationHandlerExecutor, earlySpinner, workloadStartTime, executionWindowDuration);
    }

    public void executeWorkload() throws WorkloadException {
        if (showStatus) workloadStatusThread.start();
        AtomicBoolean asyncHandlersFinished = preciseIndividualAsyncOperationStreamExecutorService.execute();
        AtomicBoolean blockingHandlersFinished = preciseIndividualBlockingOperationStreamExecutorService.execute();
        AtomicBoolean windowedHandlersFinished = uniformWindowedOperationStreamExecutorService.execute();

        AtomicBoolean[] executorFinishedFlags = new AtomicBoolean[]{asyncHandlersFinished, blockingHandlersFinished, windowedHandlersFinished};
        while (true) {
            if (errorReporter.errorEncountered()) break;
            for (int i = 0; i < executorFinishedFlags.length; i++) {
                if (null != executorFinishedFlags[i] && executorFinishedFlags[i].get()) executorFinishedFlags[i] = null;
            }
            boolean terminate = true;
            for (int i = 0; i < executorFinishedFlags.length; i++) {
                if (null != executorFinishedFlags[i]) terminate = false;
            }
            if (terminate) break;
        }

        while (true) {
            if (errorReporter.errorEncountered())
                throw new WorkloadException(String.format("Error encountered while running workload\n%s", errorReporter.toString()));
            if (asyncHandlersFinished.get() && blockingHandlersFinished.get() && windowedHandlersFinished.get())
                break;
        }

        try {
            preciseIndividualAsyncOperationStreamExecutorService.shutdown();
            preciseIndividualBlockingOperationStreamExecutorService.shutdown();
            uniformWindowedOperationStreamExecutorService.shutdown();
            // all executors have completed by this stage, there's no reason why this should not work
            operationHandlerExecutor.shutdown(WAIT_DURATION_FOR_OPERATION_HANDLER_EXECUTOR_TO_SHUTDOWN);
        } catch (OperationHandlerExecutorException e) {
            throw new WorkloadException("Encountered error while shutting down operation handler executor", e);
        }

        if (errorReporter.errorEncountered()) {
            throw new WorkloadException(String.format("Encountered error while running workload. Driver terminating.\n%s", errorReporter.toString()));
        }

        if (showStatus) workloadStatusThread.interrupt();
    }
}
