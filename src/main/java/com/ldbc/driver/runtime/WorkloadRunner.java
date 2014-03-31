package com.ldbc.driver.runtime;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.ldbc.driver.*;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.coordination.ThreadedQueuedConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.executor.OperationHandlerExecutor;
import com.ldbc.driver.runtime.executor.PreciseIndividualAsyncOperationStreamExecutorService;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.ErrorReportingExecutionDelayPolicy;
import com.ldbc.driver.runtime.scheduling.ExecutionDelayPolicy;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.runtime.scheduling.SpinnerCheck;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class WorkloadRunner {
    private static Logger logger = Logger.getLogger(WorkloadRunner.class);

    // TODO add this to config?
    public static final Duration DEFAULT_TOLERATED_OPERATION_START_TIME_DELAY = Duration.fromSeconds(1);
    // TODO add this to config
    private final Duration DEFAULT_STATUS_UPDATE_INTERVAL = Duration.fromSeconds(2);
    // TODO tune. perhaps a method that during initialization runs an ExecutionService and measures delay
    private final Duration SPINNER_OFFSET_DURATION = Duration.fromMilli(100);

    /**
     * TODO update confluence with this information
     * Spinner & Executor combinations should have the following functionality:
     * <p/>
     * ~~~~~ Definitions ~~~~~
     * <p/>
     * - 0 < window.size <= deltaT
     * <p/>
     * ~~~~~ Components ~~~~~
     * <p/>
     * - TimeMapper TODO impl, test
     * --- "maps times from workload definition into real time, i.e., by applying offsets and compression/expansion"
     * - OperationWindowTaker
     * --- "from the stream, retrieves a group of OperationHandlers that are ready to be scheduled for execution"
     * - Scheduler
     * --- "reassigns scheduled start times to a group of OperationHandlers using configurable policy, e.g., uniform within window"
     * --- takes group of operations (Iterator of OperationHandlers), those in current window, as input
     * --- optionally modifies operation.scheduledStartTime for those operations
     * --- returns group of operations (Iterator of OperationHandlers) SORTED ASCENDING by scheduledStartTime
     * - OperationHandlerExecutor
     * --- "executes OperationHandlers according to their scheduledStartTime"
     * - Spinner TODO test
     * --- "used by OperationHandlerExecutor to know when an OperationHandler's scheduledStartTime has arrived"
     * - FailurePolicy
     * <p/>
     * ~~~~~ Strategies ~~~~~
     * <p/>
     * - PreciseAsynchronous:
     * --- operation sent to scheduler:
     * ------ never, scheduledStartTime is not modified in this execution strategy
     * --- operation sent to executor:
     * ------ time == operation.scheduledTime
     * --- number of executing operations:
     * ------ unbounded
     * --- max operation runtime:
     * ------ unbounded
     * --- failure:
     * ------ operation starts executing later than scheduledTime + toleratedDelay
     * -- GCT checked:
     * ------ never(?)
     * <p/>
     * - PreciseSynchronous: TODO
     * --- operation sent to scheduler:
     * ------ never, scheduledStartTime is not modified in this execution strategy
     * --- operation sent to executor:
     * ------ time == operation.scheduledTime
     * ------ previous operation completed execution
     * --- number of concurrent executing operations:
     * ------ 1
     * --- max allowed operation runtime:
     * ------ nextOperation.scheduledTime - currentOperation.scheduledTime
     * --- failure:
     * ------ operation starts executing later than scheduledTime + toleratedDelay
     * ------ TODO careful with this implementation, if currentOperation never terminates the driver may never detect failure
     * ------ TODO use future with timeout to solve that issue?
     * -- GCT checked:
     * ------ time == operation.scheduledTime
     * <p/>
     * - WindowedAsynchronous:
     * --- operation sent to scheduler:
     * ------ window.startTime <= scheduledTime < window.startTime + window.size
     * ------ GCT <= scheduledTime
     * --- operation sent to executor:
     * ------ window.startTime <= operation.startTime < window.startTime + window.size
     * --- number of concurrent executing operations:
     * ------ number of operations within the time window
     * --- max allowed operation runtime:
     * ------ (window.startTime + window.size) - operation.actualStartTime
     * --- failure:
     * ------ "starts too late": window.startTime > operation.actualStartTime || operation.actualStartTime >= window.startTime + window.size
     * ------ "finishes too late" operation.actualStartTime + operation.runTime >= window.startTime + window.size
     * -- GCT checked:
     * ------ time == window.startTime
     */
    private final Spinner exactSpinner;
    private final Spinner slightlyEarlySpinner;
    private final WorkloadStatusThread workloadStatusThread;
    private final boolean showStatus;
    private final ConcurrentCompletionTimeService completionTimeService;
    private final Iterable<OperationHandler<?>> handlers;
    private final ConcurrentErrorReporter errorReporter;
    private final Duration gctDeltaTime;
    private final PreciseIndividualAsyncOperationStreamExecutorService preciseIndividualAsyncOperationStreamExecutorService;

    public WorkloadRunner(Db db,
                          Iterator<Operation<?>> operations,
                          Map<Class<? extends Operation<?>>, OperationClassification> operationClassificationMapping,
                          boolean showStatus,
                          int threadCount,
                          ConcurrentMetricsService metricsService,
                          ConcurrentErrorReporter errorReporter,
                          Duration gctDeltaTime) throws WorkloadException {
        this.showStatus = showStatus;
        this.errorReporter = errorReporter;
        this.gctDeltaTime = gctDeltaTime;

        // TODO make ExecutionDelayPolicy configurable
        // TODO have different ExecutionDelayPolicies for different components?
        // TODO have different error reporters for different components?
        ExecutionDelayPolicy executionDelayPolicy = new ErrorReportingExecutionDelayPolicy(DEFAULT_TOLERATED_OPERATION_START_TIME_DELAY, errorReporter);

        this.exactSpinner = new Spinner(executionDelayPolicy);
        this.slightlyEarlySpinner = new Spinner(executionDelayPolicy, SPINNER_OFFSET_DURATION);
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

        // TODO split stream and assign to ExecutorServices, CompletionTimeValidators, CompletionTimeServices, ErrorReporters as appropriate

        // Map operation stream to operation handler stream and materialize to list, to avoid doing so at runtime
        // TODO ideally the stream would only be materialized once, and this will have to be during splitting anyway
        this.handlers = ImmutableList.copyOf(operationsToOperationHandlers(
                operations,
                db,
                exactSpinner,
                completionTimeService,
                errorReporter,
                metricsService));

        // Set GCT to just before scheduled start time of earliest operation in process's stream
        try {
            // TODO find better way to define initialGct, this method will not work with multiple processes
            Time initialGct = handlers.iterator().next().operation().scheduledStartTime().minus(Duration.fromMilli(1));
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

        // TODO implement Sync Executor Service

        // TODO provide different OperationHandlerExecutor instances to different ExecutorServices to have more control over how many resources each ExecutorService can consume?
        OperationHandlerExecutor operationHandlerExecutor_NEW = new com.ldbc.driver.runtime.executor.ThreadPoolOperationHandlerExecutor(threadCount);
        this.preciseIndividualAsyncOperationStreamExecutorService = new PreciseIndividualAsyncOperationStreamExecutorService(errorReporter, completionTimeService, handlers.iterator(), slightlyEarlySpinner, operationHandlerExecutor_NEW);
    }

    public void executeWorkload() throws WorkloadException {
        // TODO need to add something like this to ConcurrentMetricsService
        //  metricsService.setStartTime(Time.now());

        if (showStatus) workloadStatusThread.start();
        AtomicBoolean finished = preciseIndividualAsyncOperationStreamExecutorService.execute();
        while (false == finished.get()) {
            if (errorReporter.errorEncountered())
                break;
        }
        // TODO cleanup everything properly first?
        if (errorReporter.errorEncountered()) {
            String errMsg = String.format("Encountered error while running workload. Driver terminating.\n%s", errorReporter.toString());
            throw new WorkloadException(errMsg);
        }
        preciseIndividualAsyncOperationStreamExecutorService.shutdown();

        // TODO if OperationHandlerExecutor instances are given to Executor Services then they must be shutdown here too

        // TODO only shutdown when terminate events comes in, because don't know when other clients will finish
        // TODO send event to coordinator information that this client has finished

        if (showStatus) workloadStatusThread.interrupt();
    }

    // TODO some only read from GCT while others read and write, think of way to do this
    // TODO perhaps by having Completion Time Service implementations that do nothing when you WRITE
    // TODO depending on operation.type() select appropriate Completion Time Service to pass to operationHandler
    // TODO ReadOnlyCompletionTimeService would be given to the operation handlers that shouldn't modify GCT
    //
    // TODO add appropriate checks
    private Iterator<OperationHandler<?>> operationsToOperationHandlers(Iterator<Operation<?>> operations,
                                                                        final Db db,
                                                                        final Spinner spinner,
                                                                        final ConcurrentCompletionTimeService completionTimeService,
                                                                        final ConcurrentErrorReporter errorReporter,
                                                                        final ConcurrentMetricsService metricsService,
                                                                        final SpinnerCheck... checks)
            throws WorkloadException {
        try {
            return Iterators.transform(operations, new Function<Operation<?>, OperationHandler<?>>() {
                @Override
                public OperationHandler<?> apply(Operation<?> operation) {
                    try {
                        OperationHandler<?> operationHandler = db.getOperationHandler(operation);
                        operationHandler.init(
                                spinner,
                                operation,
                                completionTimeService,
                                errorReporter,
                                metricsService);
                        // TODO only do below for streams that need to read GCT
//                        Duration gctDeltaDuration = null;
//                        operationHandler.addCheck(new GctCheck(completionTimeService, gctDeltaDuration, operation, errorReporter));
                        for (SpinnerCheck check : checks)
                            operationHandler.addCheck(check);
                        return operationHandler;
                    } catch (Exception e) {
                        throw new RuntimeException(e.getCause());
                    }
                }
            });
        } catch (Exception e) {
            String errMsg = "Error encountered while transforming Operation stream to OperationHandler stream";
            throw new WorkloadException(errMsg, e.getCause());
        }
    }

}
