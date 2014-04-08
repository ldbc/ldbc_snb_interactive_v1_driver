package com.ldbc.driver.runtime;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.ldbc.driver.*;
import com.ldbc.driver.control.ConcurrentControlService;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.coordination.ReadOnlyConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.coordination.ThreadedQueuedConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.executor.*;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.ErrorReportingExecutionDelayPolicy;
import com.ldbc.driver.runtime.scheduling.ExecutionDelayPolicy;
import com.ldbc.driver.runtime.scheduling.GctCheck;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.runtime.streams.IteratorSplitter;
import com.ldbc.driver.runtime.streams.IteratorSplittingException;
import com.ldbc.driver.runtime.streams.SplitDefinition;
import com.ldbc.driver.runtime.streams.SplitResult;
import com.ldbc.driver.temporal.Duration;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class WorkloadRunner {
    private final Duration DEFAULT_STATUS_UPDATE_INTERVAL = Duration.fromSeconds(2);
    private final Duration SPINNER_OFFSET_DURATION = Duration.fromMilli(100);

    private final Spinner exactSpinner;
    private final Spinner earlySpinner;

    // TODO make service and inject into workload runner
    private final WorkloadStatusThread workloadStatusThread;

    private final ConcurrentControlService controlService;
    private final ConcurrentCompletionTimeService completionTimeService;
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
                          ConcurrentErrorReporter errorReporter) throws WorkloadException {
        this.controlService = controlService;
        this.errorReporter = errorReporter;

        ExecutionDelayPolicy executionDelayPolicy = new ErrorReportingExecutionDelayPolicy(controlService.configuration().toleratedExecutionDelay(), errorReporter);

        this.exactSpinner = new Spinner(executionDelayPolicy);
        this.earlySpinner = new Spinner(executionDelayPolicy, SPINNER_OFFSET_DURATION);
        this.workloadStatusThread = new WorkloadStatusThread(DEFAULT_STATUS_UPDATE_INTERVAL, metricsService, errorReporter);

        // Create GCT maintenance thread
        try {
            completionTimeService = new ThreadedQueuedConcurrentCompletionTimeService(controlService.configuration().peerIds(), errorReporter);
        } catch (Exception e) {
            throw new WorkloadException(
                    String.format("Error while instantiating Completion Time Service with peer IDs %s",
                            controlService.configuration().peerIds().toString()),
                    e.getCause());
        }

        // Set GCT to just before scheduled start time of earliest operation in process's stream
        try {
            completionTimeService.submitInitiatedTime(controlService.workloadStartTime());
            completionTimeService.submitCompletedTime(controlService.workloadStartTime());
            for (String peerId : controlService.configuration().peerIds()) {
                completionTimeService.submitExternalCompletionTime(peerId, controlService.workloadStartTime());
            }
            // Wait for workloadStartTime to be applied
            if (false == completionTimeService.globalCompletionTimeFuture().get().equals(controlService.workloadStartTime())) {
                throw new WorkloadException("Completion Time future failed to return expected value");
            }
        } catch (Exception e) {
            throw new WorkloadException("Error while read/writing Completion Time Service", e.getCause());
        }

        Iterable<Operation<?>> windowedOperations;
        Iterable<Operation<?>> blockingOperations;
        Iterable<Operation<?>> asynchronousOperations;
        try {
            IteratorSplitter<Operation<?>> splitter = new IteratorSplitter<Operation<?>>(IteratorSplitter.UnmappedItemPolicy.ABORT);
            SplitDefinition<Operation<?>> windowed = new SplitDefinition<Operation<?>>(operationTypesBySchedulingMode(operationClassifications, OperationClassification.SchedulingMode.WINDOWED));
            SplitDefinition<Operation<?>> blocking = new SplitDefinition<Operation<?>>(operationTypesBySchedulingMode(operationClassifications, OperationClassification.SchedulingMode.INDIVIDUAL_BLOCKING));
            SplitDefinition<Operation<?>> asynchronous = new SplitDefinition<Operation<?>>(operationTypesBySchedulingMode(operationClassifications, OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC));
            SplitResult splits = splitter.split(operations, windowed, blocking, asynchronous);
            windowedOperations = splits.getSplitFor(windowed);
            blockingOperations = splits.getSplitFor(blocking);
            asynchronousOperations = splits.getSplitFor(asynchronous);
        } catch (IteratorSplittingException e) {
            throw new WorkloadException(
                    String.format("Error while splitting operation stream by scheduling mode\n%s", ConcurrentErrorReporter.stackTraceToString(e)),
                    e.getCause());
        }

        Iterable<OperationHandler<?>> windowedHandlers =
                operationsToHandlers(
                        windowedOperations,
                        db,
                        exactSpinner,
                        completionTimeService,
                        errorReporter,
                        metricsService,
                        controlService.configuration().gctDeltaDuration(),
                        operationClassifications);
        Iterable<OperationHandler<?>> blockingHandlers =
                operationsToHandlers(
                        blockingOperations,
                        db,
                        exactSpinner,
                        completionTimeService,
                        errorReporter,
                        metricsService,
                        controlService.configuration().gctDeltaDuration(),
                        operationClassifications);
        Iterable<OperationHandler<?>> asynchronousHandlers =
                operationsToHandlers(
                        asynchronousOperations,
                        db,
                        exactSpinner,
                        completionTimeService,
                        errorReporter,
                        metricsService,
                        controlService.configuration().gctDeltaDuration(),
                        operationClassifications);

        this.operationHandlerExecutor = new ThreadPoolOperationHandlerExecutor(controlService.configuration().threadCount());
        this.preciseIndividualAsyncOperationStreamExecutorService = new PreciseIndividualAsyncOperationStreamExecutorService(
                errorReporter, completionTimeService, asynchronousHandlers.iterator(), earlySpinner, operationHandlerExecutor);
        this.preciseIndividualBlockingOperationStreamExecutorService = new PreciseIndividualBlockingOperationStreamExecutorService(
                errorReporter, completionTimeService, blockingHandlers.iterator(), earlySpinner, operationHandlerExecutor);
        // TODO better way of setting window size. it does not need to equal DeltaT, it can be smaller. where to set? how to set?
        Duration windowSize = controlService.configuration().gctDeltaDuration();
        this.uniformWindowedOperationStreamExecutorService = new UniformWindowedOperationStreamExecutorService(
                errorReporter, completionTimeService, windowedHandlers.iterator(), operationHandlerExecutor, earlySpinner, controlService.workloadStartTime(), windowSize);
    }

    public void executeWorkload() throws WorkloadException {
        // TODO revise if this necessary here, and if not where??
        controlService.waitForCommandToExecuteWorkload();

        if (controlService.configuration().isShowStatus()) workloadStatusThread.start();
        AtomicBoolean asyncHandlersFinished = preciseIndividualAsyncOperationStreamExecutorService.execute();
        AtomicBoolean blockingHandlersFinished = preciseIndividualBlockingOperationStreamExecutorService.execute();
        AtomicBoolean windowedHandlersFinished = uniformWindowedOperationStreamExecutorService.execute();

        List<AtomicBoolean> executorFinishedFlags = Lists.newArrayList(asyncHandlersFinished, blockingHandlersFinished, windowedHandlersFinished);

        while (false == executorFinishedFlags.isEmpty()) {
            List<AtomicBoolean> executorFlagsToRemove = new ArrayList<AtomicBoolean>();
            for (AtomicBoolean executorFinishedFlag : executorFinishedFlags)
                if (executorFinishedFlag.get()) executorFlagsToRemove.add(executorFinishedFlag);
            for (AtomicBoolean executorFlagToRemove : executorFlagsToRemove)
                executorFinishedFlags.remove(executorFlagToRemove);
            if (errorReporter.errorEncountered())
                break;
        }

        // TODO cleanup everything properly first? what needs to be cleaned up?
        if (errorReporter.errorEncountered()) {
            throw new WorkloadException(String.format("Encountered error while running workload. Driver terminating.\n%s", errorReporter.toString()));
        }

        // TODO should executors wait for all operations to terminate before returning?
        preciseIndividualAsyncOperationStreamExecutorService.shutdown();
        preciseIndividualBlockingOperationStreamExecutorService.shutdown();
        uniformWindowedOperationStreamExecutorService.shutdown();

        // TODO if multiple executors are used (different executors for different executor services) shut them all down here
        try {
            // TODO this is total bullshit, need a better way of doing this. should be handled by executor services already
            this.operationHandlerExecutor.shutdown(Duration.fromSeconds(3600));
        } catch (OperationHandlerExecutorException e) {
            throw new WorkloadException("Encountered error while shutting down operation handler executor", e);
        }

        // TODO make status reporting service. this could report to coordinator. it could also report to a local console printer.
        if (controlService.configuration().isShowStatus()) workloadStatusThread.interrupt();

        controlService.waitForAllToCompleteExecutingWorkload();
    }

    private Iterable<OperationHandler<?>> operationsToHandlers(Iterable<Operation<?>> operations,
                                                               final Db db,
                                                               final Spinner spinner,
                                                               final ConcurrentCompletionTimeService completionTimeService,
                                                               final ConcurrentErrorReporter errorReporter,
                                                               final ConcurrentMetricsService metricsService,
                                                               final Duration gctDeltaDuration,
                                                               final Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications) throws WorkloadException {
        try {
            return Iterables.transform(operations, new Function<Operation<?>, OperationHandler<?>>() {
                @Override
                public OperationHandler<?> apply(Operation<?> operation) {
                    try {
                        OperationHandler<?> operationHandler = db.getOperationHandler(operation);
                        switch (operationClassifications.get(operation.getClass()).gctMode()) {
                            case READ_WRITE:
                                operationHandler.init(spinner, operation, completionTimeService, errorReporter, metricsService);
                                operationHandler.addCheck(new GctCheck(completionTimeService, gctDeltaDuration, operation, errorReporter));
                                break;
                            case READ:
                                operationHandler.init(spinner, operation, new ReadOnlyConcurrentCompletionTimeService(completionTimeService), errorReporter, metricsService);
                                operationHandler.addCheck(new GctCheck(completionTimeService, gctDeltaDuration, operation, errorReporter));
                                break;
                            case NONE:
                                operationHandler.init(spinner, operation, new ReadOnlyConcurrentCompletionTimeService(completionTimeService), errorReporter, metricsService);
                                break;
                            default:
                                throw new WorkloadException(String.format("Unrecognized GctMode: %s", operationClassifications.get(operation.getClass()).gctMode()));
                        }
                        return operationHandler;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        } catch (Exception e) {
            throw new WorkloadException("Error encountered while transforming Operation stream to OperationHandler stream", e.getCause());
        }
    }

    private Class<Operation<?>>[] operationTypesBySchedulingMode(Map<Class<? extends Operation<?>>, OperationClassification> operationClassificationMapping,
                                                                 OperationClassification.SchedulingMode schedulingMode) {
        List<Class<? extends Operation<?>>> operationsBySchedulingMode = new ArrayList<Class<? extends Operation<?>>>();
        for (Map.Entry<Class<? extends Operation<?>>, OperationClassification> operationAndClassification : operationClassificationMapping.entrySet()) {
            if (operationAndClassification.getValue().schedulingMode().equals(schedulingMode))
                operationsBySchedulingMode.add(operationAndClassification.getKey());
        }
        return operationsBySchedulingMode.toArray(new Class[operationsBySchedulingMode.size()]);
    }
}
