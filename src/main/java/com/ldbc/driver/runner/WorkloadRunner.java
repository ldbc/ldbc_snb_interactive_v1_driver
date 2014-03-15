package com.ldbc.driver.runner;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.ldbc.driver.*;
import com.ldbc.driver.coordination.CompletionTimeException;
import com.ldbc.driver.coordination.CompletionTimeService;
import com.ldbc.driver.coordination.ThreadedQueuedCompletionTimeService;
import com.ldbc.driver.metrics.WorkloadMetricsManager;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.OperationIteratorConverter;
import org.apache.log4j.Logger;

import java.util.*;

public class WorkloadRunner {
    private static Logger logger = Logger.getLogger(WorkloadRunner.class);

    public static final Duration DEFAULT_TOLERATED_OPERATION_START_TIME_DELAY = Duration.fromSeconds(1);
    private final Duration DEFAULT_STATUS_UPDATE_INTERVAL = Duration.fromSeconds(2);
    private final boolean IGNORE_SCHEDULED_START_TIME = false;
    private final int COMPLETION_TIME_QUEUE_CAPACITY = 1024;
    private final Duration SPINNER_OFFSET_DURATION = Duration.fromMilli(100);

    private final Db db;
    private final Spinner exactSpinner;
    private final Spinner slightlyEarlySpinner;
    private final OperationHandlerExecutor operationHandlerExecutor;
    // TODO make MetricsLoggingService where Threading is not visible
    private final MetricsLoggingThread metricsLoggingThread;
    private final WorkloadMetricsManager metricsManager;
    // TODO make WorkloadStatusService where Threading is not visible
    private final WorkloadStatusThread workloadStatusThread;
    private final boolean showStatus;
    private final CompletionTimeService completionTimeService;
    private final Iterable<OperationHandler<?>> operationHandlers;
    private final ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

    public WorkloadRunner(Db db, Iterator<Operation<?>> operations, Map<Class<?>, OperationClassification> mapping,
                          boolean showStatus, int threadCount,
                          WorkloadMetricsManager metricsManager) throws WorkloadException {

        // How to use OperationIteratorConverter:
        // OperationIteratorConverter converter = new OperationIteratorConverter(operations, mapping);
        // converter.start();
        // try {
        //     Thread.sleep(1000);
        //     // iterator.next() is non-blocking, therefore wait a while before iterating over the produced streams
        // } catch (InterruptedException e) {
        //     Thread.currentThread().interrupt();
        // }
        // Operation<?> op1 = converter.getIterator(OperationClassification.WindowFalse_GCTRead).next();
        // System.out.println("op1: " + op1);
        // try {
        //    converter.join();
        // } catch (InterruptedException e) {
        //     String errMsg = "Error encountered while waiting for stream converter thread to finish";
        //     logger.error(errMsg, e);
        //     throw new WorkloadException(errMsg, e.getCause());
        // }

        this.db = db;
        this.metricsManager = metricsManager;
        this.showStatus = showStatus;

        // TODO make OperationSchedulingPolicy configurable
        OperationSchedulingPolicy operationSchedulingPolicy = new LoggingOperationSchedulingPolicy(
                DEFAULT_TOLERATED_OPERATION_START_TIME_DELAY, IGNORE_SCHEDULED_START_TIME);

        this.exactSpinner = new Spinner(operationSchedulingPolicy);
        this.slightlyEarlySpinner = new Spinner(operationSchedulingPolicy, SPINNER_OFFSET_DURATION);

        // Map operation stream to operation handler stream and materialize to list, to avoid doing so at runtime
        this.operationHandlers = ImmutableList.copyOf(operationsToOperationHandlers(operations, exactSpinner));

        // Create operation handler executor/scheduler component
        this.operationHandlerExecutor = new ThreadPoolOperationHandlerExecutor(threadCount, errorReporter);

        // Create benchmark metrics maintenance thread
        this.metricsLoggingThread = new MetricsLoggingThread(operationHandlerExecutor, metricsManager, errorReporter);

        // Create status thread
        Duration statusInterval = DEFAULT_STATUS_UPDATE_INTERVAL;
        this.workloadStatusThread = new WorkloadStatusThread(statusInterval, metricsManager, errorReporter);

        // Create GCT maintenance thread
        // TODO get peerIds from somewhere
        List<String> peerIds = new ArrayList<String>();
        try {
            // TODO once CompletionTimeService implementations support setting initial GCT
            // Set GCT to just before scheduled start time of earliest operation in process's stream
            // Time initialGct = operationHandlers.iterator().next().getOperation().scheduledStartTime().minus(Duration.fromMilli(1));

            // TODO remember NaiveSynchronizedCompletionTimeService can be used instead
            completionTimeService = new ThreadedQueuedCompletionTimeService(peerIds, errorReporter);
        } catch (CompletionTimeException e) {
            throw new WorkloadException(
                    String.format("Error while instantiating completionTimeService with peerIds %s",
                            peerIds.toString()),
                    e.getCause());
        }
    }

    public void run() throws WorkloadException {
        metricsManager.setStartTime(Time.now());
        metricsLoggingThread.start();
        if (showStatus) workloadStatusThread.start();
        for (OperationHandler<?> operationHandler : operationHandlers) {
            // Schedule slightly early to account for context switch latency
            // Internally, OperationHandler will schedule at exact scheduled start time
            slightlyEarlySpinner.waitForScheduledStartTime(operationHandler.getOperation());
            operationHandlerExecutor.execute(operationHandler);
            if (errorReporter.errorEncountered()) {
                String errMsg = String.format("Benchmark terminating due to fatal error!\n" +
                        "Number of operations completed before termination: %s\n%s",
                        metricsManager.getMeasurementCount(), errorReporter.toString());
                throw new WorkloadException(errMsg);
            }
        }

        try {
            metricsLoggingThread.finishLoggingRemainingResults();
            metricsLoggingThread.join();
            if (showStatus) workloadStatusThread.interrupt();
        } catch (InterruptedException e) {
            String errMsg = "Error encountered while waiting for logging thread to finish";
            logger.error(errMsg, e);
            throw new WorkloadException(errMsg, e.getCause());
        }

        try {
            operationHandlerExecutor.shutdown();
        } catch (OperationHandlerExecutorException e) {
            String errMsg = "Error encountered while shutting down operation handler executor";
            logger.error(errMsg, e);
            throw new WorkloadException(errMsg, e.getCause());
        }
    }

    // TODO pass completionTimeService to OperationHandlers
    // TODO some only read from GCT while others read and write, think of way to do this
    // TODO perhaps by having completionTimeService implementations that do nothing when you WRITE
    private Iterator<OperationHandler<?>> operationsToOperationHandlers(Iterator<Operation<?>> operations, final Spinner spinner) throws WorkloadException {
        try {
            return Iterators.transform(operations, new Function<Operation<?>, OperationHandler<?>>() {
                @Override
                public OperationHandler<?> apply(Operation<?> operation) {
                    try {
                        OperationHandler<?> operationHandler = db.getOperationHandler(operation);
                        operationHandler.init(spinner, operation);
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
