package com.ldbc.driver.runtime;

import com.ldbc.driver.*;
import com.ldbc.driver.control.ConcurrentControlService;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.executor.*;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.ErrorReportingExecutionDelayPolicy;
import com.ldbc.driver.runtime.scheduling.ExecutionDelayPolicy;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.runtime.streams.IteratorSplitter;
import com.ldbc.driver.runtime.streams.IteratorSplittingException;
import com.ldbc.driver.runtime.streams.SplitDefinition;
import com.ldbc.driver.runtime.streams.SplitResult;
import com.ldbc.driver.temporal.Duration;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ldbc.driver.OperationClassification.SchedulingMode;

public class WorkloadRunner {
    private final Duration DEFAULT_STATUS_UPDATE_INTERVAL = Duration.fromSeconds(2);
    private final Duration SPINNER_OFFSET_DURATION = Duration.fromMilli(100);

    private final Spinner exactSpinner;
    private final Spinner earlySpinner;

    // TODO make service and inject into workload runner. this could report to coordinator OR a local console printer, for example
    private final WorkloadStatusThread workloadStatusThread;

    private final ConcurrentControlService controlService;
    private final ConcurrentErrorReporter errorReporter;

    private final OperationHandlerExecutor operationHandlerExecutor;

    private final PreciseIndividualAsyncOperationStreamExecutorService preciseIndividualAsyncOperationStreamExecutorService;
    private final PreciseIndividualBlockingOperationStreamExecutorService preciseIndividualBlockingOperationStreamExecutorService;
    private final UniformWindowedOperationStreamExecutorService uniformWindowedOperationStreamExecutorService;

    public WorkloadRunner(ConcurrentControlService controlService,
                          Db db,
                          Iterator<Operation<?>> operations,
                          Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications,
                          ConcurrentMetricsService metricsService,
                          ConcurrentErrorReporter errorReporter,
                          ConcurrentCompletionTimeService completionTimeService) throws WorkloadException {
        this.controlService = controlService;
        this.errorReporter = errorReporter;

        ExecutionDelayPolicy executionDelayPolicy = new ErrorReportingExecutionDelayPolicy(controlService.configuration().toleratedExecutionDelay(), errorReporter);

        // TODO for the spinner sent to Window scheduler allow delay to reach to the end of window?

        this.exactSpinner = new Spinner(executionDelayPolicy);
        this.earlySpinner = new Spinner(executionDelayPolicy, SPINNER_OFFSET_DURATION);
        this.workloadStatusThread = new WorkloadStatusThread(DEFAULT_STATUS_UPDATE_INTERVAL, metricsService, errorReporter);

        Iterator<Operation<?>> windowedOperations;
        Iterator<Operation<?>> blockingOperations;
        Iterator<Operation<?>> asynchronousOperations;
        try {
            IteratorSplitter<Operation<?>> splitter = new IteratorSplitter<Operation<?>>(IteratorSplitter.UnmappedItemPolicy.ABORT);
            SplitDefinition<Operation<?>> windowed = new SplitDefinition<Operation<?>>(Workload.operationTypesBySchedulingMode(operationClassifications, SchedulingMode.WINDOWED));
            SplitDefinition<Operation<?>> blocking = new SplitDefinition<Operation<?>>(Workload.operationTypesBySchedulingMode(operationClassifications, SchedulingMode.INDIVIDUAL_BLOCKING));
            SplitDefinition<Operation<?>> asynchronous = new SplitDefinition<Operation<?>>(Workload.operationTypesBySchedulingMode(operationClassifications, SchedulingMode.INDIVIDUAL_ASYNC));
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
                db,
                exactSpinner,
                completionTimeService,
                errorReporter,
                metricsService,
                controlService.configuration().gctDeltaDuration(),
                operationClassifications);
        Iterator<OperationHandler<?>> windowedHandlers = operationsToHandlers.transform(windowedOperations);
        Iterator<OperationHandler<?>> blockingHandlers = operationsToHandlers.transform(blockingOperations);
        Iterator<OperationHandler<?>> asynchronousHandlers = operationsToHandlers.transform(asynchronousOperations);

        // TODO these executor services should all be using different gct services and sharing gct via external ct [MUST]
        // TODO This lesson needs to be written to Confluence too

        this.operationHandlerExecutor = new ThreadPoolOperationHandlerExecutor(controlService.configuration().threadCount());
        this.preciseIndividualAsyncOperationStreamExecutorService = new PreciseIndividualAsyncOperationStreamExecutorService(
                errorReporter, completionTimeService, asynchronousHandlers, earlySpinner, operationHandlerExecutor);
        this.preciseIndividualBlockingOperationStreamExecutorService = new PreciseIndividualBlockingOperationStreamExecutorService(
                errorReporter, completionTimeService, blockingHandlers, earlySpinner, operationHandlerExecutor);
        // TODO better way of setting window size. it does not need to equal DeltaT, it can be smaller. where to set? how to set?
        Duration windowSize = controlService.configuration().gctDeltaDuration();
        this.uniformWindowedOperationStreamExecutorService = new UniformWindowedOperationStreamExecutorService(
                errorReporter, completionTimeService, windowedHandlers, operationHandlerExecutor, earlySpinner, controlService.workloadStartTime(), windowSize);
    }

    public void executeWorkload() throws WorkloadException {
        // TODO revise if this necessary here, and if not where??
        controlService.waitForCommandToExecuteWorkload();

        if (controlService.configuration().showStatus()) workloadStatusThread.start();
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
            operationHandlerExecutor.shutdown(Duration.fromMilli(1));
        } catch (OperationHandlerExecutorException e) {
            throw new WorkloadException("Encountered error while shutting down operation handler executor", e);
        }

        if (errorReporter.errorEncountered()) {
            throw new WorkloadException(String.format("Encountered error while running workload. Driver terminating.\n%s", errorReporter.toString()));
        }

        if (controlService.configuration().showStatus()) workloadStatusThread.interrupt();

        controlService.waitForAllToCompleteExecutingWorkload();
    }
}
