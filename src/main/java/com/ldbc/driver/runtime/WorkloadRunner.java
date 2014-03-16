package com.ldbc.driver.runtime;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.ldbc.driver.*;
import com.ldbc.driver.generator.Window;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.coordination.ThreadedQueuedConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.error.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.error.ExecutionDelayPolicy;
import com.ldbc.driver.runtime.error.LoggingExecutionDelayPolicy;
import com.ldbc.driver.runtime.executor.OperationHandlerExecutor;
import com.ldbc.driver.runtime.executor.OperationHandlerExecutorException;
import com.ldbc.driver.runtime.executor.Spinner;
import com.ldbc.driver.runtime.executor.ThreadPoolOperationHandlerExecutor;
import com.ldbc.driver.runtime.executor_NEW.OperationClassification;
import com.ldbc.driver.runtime.executor_NEW.UniformWindowedOperationStreamExecutorService;
import com.ldbc.driver.runtime.metrics_NEW.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduler.Scheduler;
import com.ldbc.driver.runtime.scheduler.UniformWindowedScheduler;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class WorkloadRunner {
    private static Logger logger = Logger.getLogger(WorkloadRunner.class);

    public static final Duration DEFAULT_TOLERATED_OPERATION_START_TIME_DELAY = Duration.fromSeconds(1);
    private final Duration DEFAULT_STATUS_UPDATE_INTERVAL = Duration.fromSeconds(2);
    // TODO this needs to be tuned perhaps a method that during initialization runs an ExecutionService and measures delay
    private final Duration SPINNER_OFFSET_DURATION = Duration.fromMilli(100);

    /**
     * TODO update confluence with this information
     * TODO implement the necessary classes for the below
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
     * - OperationWindowTaker TODO test
     * --- "from the stream, retrieves a group of OperationHandlers that are ready to be scheduled for execution"
     * - Scheduler TODO test
     * --- "reassigns scheduled start times to a group of OperationHandlers using configurable policy, e.g., uniform within window"
     * --- takes group of operations (Iterator of OperationHandlers), those in current window, as input
     * --- optionally modifies operation.scheduledStartTime for those operations
     * --- returns group of operations (Iterator of OperationHandlers) SORTED ASCENDING by scheduledStartTime
     * - OperationHandlerExecutor TODO impl, test
     * --- "executes OperationHandlers according to their scheduledStartTime"
     * - Spinner TODO impl, test
     * --- "used by OperationHandlerExecutor to know when an OperationHandler's scheduledStartTime has arrived"
     * - FailurePolicy TODO
     * --- TODO remove ability to ignore scheduledStartTime,
     * --- TODO to compensate, possibly support automatically setting it to Time.now() if it's not set
     * <p/>
     * ~~~~~ Strategies ~~~~~
     * <p/>
     * - PreciseAsynchronous:
     * --- operation sent to scheduler:
     * ------ never, scheduledStartTime is not modified in this execution strategy
     * --- operation sent to executor:
     * ------ scheduledTime
     * --- number of executing operations:
     * ------ unbounded
     * --- max operation runtime:
     * ------ unbounded
     * --- failure:
     * ------ operation starts executing later than scheduledTime + toleratedDelay
     * <p/>
     * - Synchronous:
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
     */
    private final Spinner exactSpinner;
    private final Spinner slightlyEarlySpinner;
    private final OperationHandlerExecutor operationHandlerExecutor;
    private final ConcurrentMetricsService metricsService;
    // TODO make WorkloadStatusService where Threading is not visible
    private final WorkloadStatusThread workloadStatusThread;
    private final boolean showStatus;
    private final ConcurrentCompletionTimeService concurrentCompletionTimeService;
    private final Iterable<OperationHandler<?>> operationHandlers;
    private final ConcurrentErrorReporter errorReporter;

    public WorkloadRunner(Db db,
                          Iterator<Operation<?>> operations,
                          Map<Class<?>, OperationClassification> operationClassificationMapping,
                          boolean showStatus,
                          int threadCount,
                          ConcurrentMetricsService metricsService,
                          ConcurrentErrorReporter errorReporter) throws WorkloadException {
        // TODO ===== EXPERIMENTAL - WINDOW STREAM EXECUTOR ======
        Iterator<OperationHandler<?>> handlers = null;
        Time startTime = Time.now();
        Duration windowSize = Duration.fromMilli(100);
        int threadCountX = 1;
        ConcurrentErrorReporter errorReporter1 = null;
        ConcurrentCompletionTimeService completionTimeService = null;
        Scheduler<List<OperationHandler<?>>, Window.OperationHandlerTimeRangeWindow> scheduler = new UniformWindowedScheduler();
        UniformWindowedOperationStreamExecutorService executorService = new UniformWindowedOperationStreamExecutorService(
                startTime,
                windowSize,
                threadCountX,
                errorReporter1,
                completionTimeService,
                handlers);
        executorService.execute();
        executorService.shutdown();
        // TODO ===== EXPERIMENTAL - WINDOW STREAM EXECUTOR ======

        this.metricsService = metricsService;
        this.showStatus = showStatus;
        this.errorReporter = errorReporter;

        // TODO make ExecutionDelayPolicy configurable
        // TODO make allowable delay configurable
        // TODO make ignore start time configurable
        ExecutionDelayPolicy executionDelayPolicy = new LoggingExecutionDelayPolicy(DEFAULT_TOLERATED_OPERATION_START_TIME_DELAY);

        this.exactSpinner = new Spinner(executionDelayPolicy);
        this.slightlyEarlySpinner = new Spinner(executionDelayPolicy, SPINNER_OFFSET_DURATION);

        this.operationHandlerExecutor = new ThreadPoolOperationHandlerExecutor(threadCount, errorReporter);

        Duration statusInterval = DEFAULT_STATUS_UPDATE_INTERVAL;

        this.workloadStatusThread = new WorkloadStatusThread(statusInterval, metricsService, errorReporter);

        // Create GCT maintenance thread
        // TODO get peerIds from somewhere
        List<String> peerIds = new ArrayList<String>();
        try {
            // TODO make ConcurrentCompletionTimeService implementation (NaiveSynchronized vs ThreadedQueued) configurable?
            concurrentCompletionTimeService = new ThreadedQueuedConcurrentCompletionTimeService(peerIds, errorReporter);
        } catch (Exception e) {
            throw new WorkloadException(
                    String.format("Error while instantiating concurrentCompletionTimeService with peerIds %s",
                            peerIds.toString()),
                    e.getCause());
        }

        // Map operation stream to operation handler stream and materialize to list, to avoid doing so at runtime
        this.operationHandlers = ImmutableList.copyOf(operationsToOperationHandlers(operations, db, exactSpinner, concurrentCompletionTimeService));

        // Set GCT to just before scheduled start time of earliest operation in process's stream
        try {
            // TODO find better way to define initialGct, this method will not work with multiple processes
            Time initialGct = operationHandlers.iterator().next().operation().scheduledStartTime().minus(Duration.fromMilli(1));
            concurrentCompletionTimeService.submitInitiatedTime(initialGct);
            concurrentCompletionTimeService.submitCompletedTime(initialGct);
            for (String peerId : peerIds) {
                concurrentCompletionTimeService.submitExternalCompletionTime(peerId, initialGct);
            }
            // Wait for initialGct to be applied
            if (false == concurrentCompletionTimeService.globalCompletionTimeFuture().get().equals(initialGct)) {
                throw new WorkloadException("Completion Time future failed to return expected value");
            }
        } catch (Exception e) {
            throw new WorkloadException(
                    String.format("Error while instantiating concurrentCompletionTimeService with peerIds %s",
                            peerIds.toString()),
                    e.getCause());
        }
    }

    public void run() throws WorkloadException {
        // TODO need to add something like this to ConcurrentMetricsService
        //  metricsService.setStartTime(Time.now());
        if (showStatus) workloadStatusThread.start();
        for (OperationHandler<?> operationHandler : operationHandlers) {
            // Schedule slightly early to account for context switch latency
            // Internally, OperationHandler will schedule at exact scheduled start time
            slightlyEarlySpinner.waitForScheduledStartTime(operationHandler.operation());
            // TODO submitting initiated time should probably be done by OperationHandlerExecutor
            try {
                concurrentCompletionTimeService.submitInitiatedTime(operationHandler.operation().scheduledStartTime());
            } catch (CompletionTimeException e) {
                String errMsg = String.format("Benchmark terminating due to fatal error!");
                throw new WorkloadException(errMsg, e.getCause());
            }
            operationHandlerExecutor.execute(operationHandler);
            if (errorReporter.errorEncountered()) {
                String errMsg = String.format("Benchmark terminating due to fatal error!");
                throw new WorkloadException(errMsg);
            }
        }

        // TODO only shutdown when terminate events comes in, because don't know when other clients will finish
        // TODO send event to coordinator information that this client has finished

        if (showStatus) workloadStatusThread.interrupt();

        try {
            operationHandlerExecutor.shutdown();
        } catch (OperationHandlerExecutorException e) {
            String errMsg = "Error encountered while shutting down operation handler executor";
            logger.error(errMsg, e);
            throw new WorkloadException(errMsg, e.getCause());
        }
    }

    // TODO some only read from GCT while others read and write, think of way to do this
    // TODO perhaps by having concurrentCompletionTimeService implementations that do nothing when you WRITE
    // TODO depending on operation.type() select appropriate concurrentCompletionTimeService to pass to operationHandler
    // TODO ReadOnlyCompletionTimeService would be given to the operationHandlers that shouldn't modify GCT
    private Iterator<OperationHandler<?>> operationsToOperationHandlers(Iterator<Operation<?>> operations,
                                                                        final Db db,
                                                                        final Spinner spinner,
                                                                        final ConcurrentCompletionTimeService concurrentCompletionTimeService)
            throws WorkloadException {
        try {
            return Iterators.transform(operations, new Function<Operation<?>, OperationHandler<?>>() {
                @Override
                public OperationHandler<?> apply(Operation<?> operation) {
                    try {
                        OperationHandler<?> operationHandler = db.getOperationHandler(operation);
                        operationHandler.init(spinner, operation, concurrentCompletionTimeService);
                        return operationHandler;
                    } catch (DbException e) {
                        throw new RuntimeException();
                    }
                }
            });
        } catch (RuntimeException e) {
            String errMsg = "Error encountered while transforming Operation stream to OperationHandler stream";
            logger.error(errMsg, e);
            throw new WorkloadException(errMsg, e.getCause());
        }
    }

}
