package com.ldbc.driver.runtime;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.ldbc.driver.*;
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
import com.ldbc.driver.temporal.Time;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class WorkloadRunner {
    // TODO add this to config?
    public static final Duration DEFAULT_TOLERATED_OPERATION_START_TIME_DELAY = Duration.fromSeconds(1);
    // TODO add this to config
    private final Duration DEFAULT_STATUS_UPDATE_INTERVAL = Duration.fromSeconds(2);
    // TODO tune. perhaps a method that during initialization runs an ExecutionService and measures delay
    private final Duration SPINNER_OFFSET_DURATION = Duration.fromMilli(100);

    // TODO TimeMapper "maps times from workload definition into real time, i.e., by applying offsets and compression/expansion"

    private final Spinner exactSpinner;
    private final Spinner earlySpinner;
    private final WorkloadStatusThread workloadStatusThread;
    private final boolean showStatus;
    private final ConcurrentCompletionTimeService completionTimeService;
    private final ConcurrentErrorReporter errorReporter;

    private final OperationHandlerExecutor operationHandlerExecutor;

    private final PreciseIndividualAsyncOperationStreamExecutorService preciseIndividualAsyncOperationStreamExecutorService;
    private final PreciseIndividualBlockingOperationStreamExecutorService preciseIndividualBlockingOperationStreamExecutorService;
    private final UniformWindowedOperationStreamExecutorService uniformWindowedOperationStreamExecutorService;

    public WorkloadRunner(Db db,
                          Iterator<Operation<?>> operations,
                          Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications,
                          boolean showStatus,
                          int threadCount,
                          ConcurrentMetricsService metricsService,
                          ConcurrentErrorReporter errorReporter,
                          Duration gctDelta,
                          Time initialGct) throws WorkloadException {
        this.showStatus = showStatus;
        this.errorReporter = errorReporter;

        // TODO make ExecutionDelayPolicy configurable
        // TODO have different ExecutionDelayPolicies for different components?
        // TODO have different error reporters for different components?
        ExecutionDelayPolicy executionDelayPolicy = new ErrorReportingExecutionDelayPolicy(DEFAULT_TOLERATED_OPERATION_START_TIME_DELAY, errorReporter);

        this.exactSpinner = new Spinner(executionDelayPolicy);
        this.earlySpinner = new Spinner(executionDelayPolicy, SPINNER_OFFSET_DURATION);
        this.workloadStatusThread = new WorkloadStatusThread(DEFAULT_STATUS_UPDATE_INTERVAL, metricsService, errorReporter);

        // Create GCT maintenance thread
        // TODO get peerIds from somewhere
        List<String> peerIds = new ArrayList<String>();
        try {
            // TODO make ConcurrentCompletionTimeService implementation configurable? perhaps method compares performance on target machine
            completionTimeService = new ThreadedQueuedConcurrentCompletionTimeService(peerIds, errorReporter);
        } catch (Exception e) {
            throw new WorkloadException(
                    String.format("Error while instantiating Completion Time Service with peer IDs %s",
                            peerIds.toString()),
                    e.getCause());
        }

        // Set GCT to just before scheduled start time of earliest operation in process's stream
        try {
            // TODO find better way to define initialGct, this method will not work with multiple processes
            completionTimeService.submitInitiatedTime(initialGct);
            completionTimeService.submitCompletedTime(initialGct);
            for (String peerId : peerIds) {
                completionTimeService.submitExternalCompletionTime(peerId, initialGct);
            }
            // Wait for initialGct to be applied
            if (false == completionTimeService.globalCompletionTimeFuture().get().equals(initialGct)) {
                throw new WorkloadException("Completion Time future failed to return expected value");
            }
        } catch (Exception e) {
            throw new WorkloadException(
                    String.format(
                            "Error while instantiating Completion Time Service with peer IDs %s",
                            peerIds.toString()),
                    e.getCause());
        }

        Iterable<Operation<?>> windowedOperations = null;
        Iterable<Operation<?>> blockingOperations = null;
        Iterable<Operation<?>> asynchronousOperations = null;
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
                operationsToHandlers(windowedOperations, db, exactSpinner, completionTimeService, errorReporter, metricsService, gctDelta, operationClassifications);
        Iterable<OperationHandler<?>> blockingHandlers =
                operationsToHandlers(blockingOperations, db, exactSpinner, completionTimeService, errorReporter, metricsService, gctDelta, operationClassifications);
        Iterable<OperationHandler<?>> asynchronousHandlers =
                operationsToHandlers(asynchronousOperations, db, exactSpinner, completionTimeService, errorReporter, metricsService, gctDelta, operationClassifications);

        // TODO provide different OperationHandlerExecutor instances to different ExecutorServices to have more control over how many resources each ExecutorService can consume?
        this.operationHandlerExecutor = new ThreadPoolOperationHandlerExecutor(threadCount);
        this.preciseIndividualAsyncOperationStreamExecutorService =
                new PreciseIndividualAsyncOperationStreamExecutorService(errorReporter, completionTimeService, asynchronousHandlers.iterator(), earlySpinner, operationHandlerExecutor);
        this.preciseIndividualBlockingOperationStreamExecutorService =
                new PreciseIndividualBlockingOperationStreamExecutorService(errorReporter, completionTimeService, blockingHandlers.iterator(), earlySpinner, operationHandlerExecutor);
        // TODO where should this get set from?
        Time firstWindowStartTime = null;
        // TODO where should this get set from?
        Duration windowSize = null;
        this.uniformWindowedOperationStreamExecutorService =
                new UniformWindowedOperationStreamExecutorService(errorReporter, completionTimeService, windowedHandlers.iterator(), operationHandlerExecutor, earlySpinner, firstWindowStartTime, windowSize);
    }

    public void executeWorkload() throws WorkloadException {
        // TODO need to add something like this to ConcurrentMetricsService
        //  metricsService.setStartTime(Time.now());

        if (showStatus) workloadStatusThread.start();
        AtomicBoolean asyncHandlersFinished = preciseIndividualAsyncOperationStreamExecutorService.execute();
        AtomicBoolean blockingHandlersFinished = preciseIndividualBlockingOperationStreamExecutorService.execute();
        // TODO uncomment after creation of this executor is figured out - see constructor above
//        AtomicBoolean windowedHandlersFinished = uniformWindowedOperationStreamExecutorService.execute();

        // TODO add windowed executor to list too, when it's working
        List<AtomicBoolean> executorFinishedFlags = Lists.newArrayList(asyncHandlersFinished, blockingHandlersFinished);

        while (false == executorFinishedFlags.isEmpty()) {
            List<AtomicBoolean> executorFlagsToRemove = new ArrayList<AtomicBoolean>();
            for (AtomicBoolean executorFinishedFlag : executorFinishedFlags)
                if (executorFinishedFlag.get()) executorFlagsToRemove.add(executorFinishedFlag);
            for (AtomicBoolean executorFlagToRemove : executorFlagsToRemove)
                executorFinishedFlags.remove(executorFlagToRemove);
            if (errorReporter.errorEncountered())
                break;
        }

        // TODO cleanup everything properly first?
        if (errorReporter.errorEncountered()) {
            throw new WorkloadException(String.format("Encountered error while running workload. Driver terminating.\n%s", errorReporter.toString()));
        }

        // TODO should executors wait for all operations to terminate before returning?
        preciseIndividualAsyncOperationStreamExecutorService.shutdown();
        preciseIndividualBlockingOperationStreamExecutorService.shutdown();
        // TODO uncomment when this executor is working
//        uniformWindowedOperationStreamExecutorService.shutdown();

        // TODO if multiple executors are used (different executors for different executor services) shut them all down here
        try {
            // TODO this is total bullshit, need a better way of doing this. should be handled by executor services already
            this.operationHandlerExecutor.shutdown(Duration.fromSeconds(3600));
        } catch (OperationHandlerExecutorException e) {
            throw new WorkloadException("Encountered error while shutting down operation handler executor", e);
        }

        // TODO only shutdown when terminate events comes in, because don't know when other clients will finish
        // TODO send event to coordinator information that this client has finished

        if (showStatus) workloadStatusThread.interrupt();
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
