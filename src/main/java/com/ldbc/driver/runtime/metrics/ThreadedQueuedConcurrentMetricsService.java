package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.DefaultQueues;
import com.ldbc.driver.runtime.QueueEventSubmitter;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.csv.SimpleCsvFileWriter;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ThreadedQueuedConcurrentMetricsService implements ConcurrentMetricsService {
    private static final long SHUTDOWN_WAIT_TIMEOUT_AS_MILLI = TimeUnit.MINUTES.toMillis(1);
    private static final long FUTURE_GET_TIMEOUT_AS_MILLI = TimeUnit.MINUTES.toMillis(30);

    public static final String RESULTS_LOG_FILENAME_SUFFIX = "-results_log.csv";
    public static final String RESULTS_METRICS_FILENAME_SUFFIX = "-results.json";
    public static final String RESULTS_CONFIGURATION_FILENAME_SUFFIX = "-configuration.properties";

    // TODO this could come from config, if we had a max_runtime parameter. for now, it can default to something
    public static final long DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO = TimeUnit.MINUTES.toNanos(90);

    private final TimeSource timeSource;
    private final QueueEventSubmitter<ThreadedQueuedMetricsCollectionEvent> queueEventSubmitter;
    private final AtomicLong initiatedEvents;
    private final ThreadedQueuedConcurrentMetricsServiceThread threadedQueuedConcurrentMetricsServiceThread;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    public static ThreadedQueuedConcurrentMetricsService newInstanceUsingNonBlockingBoundedQueue(TimeSource timeSource,
                                                                                                 ConcurrentErrorReporter errorReporter,
                                                                                                 TimeUnit unit,
                                                                                                 long maxRuntimeDurationAsNano,
                                                                                                 SimpleCsvFileWriter csvResultsLogWriter,
                                                                                                 Map<Integer, Class<? extends Operation<?>>> operationTypeToClassMapping) throws MetricsCollectionException {
        Queue<ThreadedQueuedMetricsCollectionEvent> queue = DefaultQueues.newNonBlockingBounded(10000);
        return new ThreadedQueuedConcurrentMetricsService(
                timeSource,
                errorReporter,
                unit,
                maxRuntimeDurationAsNano,
                queue,
                csvResultsLogWriter,
                operationTypeToClassMapping);
    }

    public static ThreadedQueuedConcurrentMetricsService newInstanceUsingBlockingBoundedQueue(TimeSource timeSource,
                                                                                              ConcurrentErrorReporter errorReporter,
                                                                                              TimeUnit unit,
                                                                                              long maxRuntimeDurationAsNano,
                                                                                              SimpleCsvFileWriter csvResultsLogWriter,
                                                                                              Map<Integer, Class<? extends Operation<?>>> operationTypeToClassMapping) throws MetricsCollectionException {
        Queue<ThreadedQueuedMetricsCollectionEvent> queue = DefaultQueues.newBlockingBounded(10000);
        return new ThreadedQueuedConcurrentMetricsService(
                timeSource,
                errorReporter,
                unit,
                maxRuntimeDurationAsNano,
                queue,
                csvResultsLogWriter,
                operationTypeToClassMapping);
    }

    private ThreadedQueuedConcurrentMetricsService(TimeSource timeSource,
                                                   ConcurrentErrorReporter errorReporter,
                                                   TimeUnit unit,
                                                   long maxRuntimeDurationAsNano,
                                                   Queue<ThreadedQueuedMetricsCollectionEvent> queue,
                                                   SimpleCsvFileWriter csvResultsLogWriter,
                                                   Map<Integer, Class<? extends Operation<?>>> operationTypeToClassMapping) throws MetricsCollectionException {
        this.timeSource = timeSource;
        this.queueEventSubmitter = QueueEventSubmitter.queueEventSubmitterFor(queue);
        this.initiatedEvents = new AtomicLong(0);
        threadedQueuedConcurrentMetricsServiceThread = new ThreadedQueuedConcurrentMetricsServiceThread(
                errorReporter,
                queue,
                csvResultsLogWriter,
                timeSource,
                unit,
                maxRuntimeDurationAsNano,
                operationTypeToClassMapping);

        threadedQueuedConcurrentMetricsServiceThread.start();
    }

    @Override
    public void submitOperationResult(
            int operationType,
            long scheduledStartTimeAsMilli,
            long actualStartTimeAsMilli,
            long runDurationAsNano,
            int resultCode
    ) throws MetricsCollectionException {
        if (shutdown.get()) {
            throw new MetricsCollectionException("Can not submit a result after calling shutdown");
        }
        try {
            initiatedEvents.incrementAndGet();
            ThreadedQueuedMetricsCollectionEvent event = new ThreadedQueuedMetricsCollectionEvent.SubmitOperationResult(
                    operationType,
                    scheduledStartTimeAsMilli,
                    actualStartTimeAsMilli,
                    runDurationAsNano,
                    resultCode
            );
            queueEventSubmitter.submitEventToQueue(event);
        } catch (InterruptedException e) {
            String errMsg = String.format(
                    "Error submitting result\n"
                            + "Operation Type: %s\n"
                            + "Scheduled Start Time Ms: %s\n"
                            + "Actual Start Time Ms: %s\n"
                            + "Duration Ns: %s\n"
                            + "Result Code: %s\n"
                    ,
                    operationType,
                    scheduledStartTimeAsMilli,
                    actualStartTimeAsMilli,
                    runDurationAsNano,
                    resultCode
            );
            throw new MetricsCollectionException(errMsg, e);
        }
    }

    @Override
    public WorkloadStatusSnapshot status() throws MetricsCollectionException {
        if (shutdown.get()) {
            throw new MetricsCollectionException("Can not read metrics status after calling shutdown");
        }
        try {
            MetricsStatusFuture statusFuture = new MetricsStatusFuture(timeSource);
            ThreadedQueuedMetricsCollectionEvent event = new ThreadedQueuedMetricsCollectionEvent.Status(statusFuture);
            queueEventSubmitter.submitEventToQueue(event);
            return statusFuture.get(FUTURE_GET_TIMEOUT_AS_MILLI, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new MetricsCollectionException("Error while submitting request for workload status", e);
        }
    }

    @Override
    public WorkloadResultsSnapshot results() throws MetricsCollectionException {
        if (shutdown.get()) {
            throw new MetricsCollectionException("Can not retrieve results after calling shutdown");
        }
        try {
            MetricsWorkloadResultFuture workloadResultFuture = new MetricsWorkloadResultFuture(timeSource);
            ThreadedQueuedMetricsCollectionEvent event = new ThreadedQueuedMetricsCollectionEvent.GetWorkloadResults(workloadResultFuture);
            queueEventSubmitter.submitEventToQueue(event);
            return workloadResultFuture.get(FUTURE_GET_TIMEOUT_AS_MILLI, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new MetricsCollectionException("Error while submitting request for workload results", e);
        }
    }

    @Override
    synchronized public void shutdown() throws MetricsCollectionException {
        if (shutdown.get())
            throw new MetricsCollectionException("Metrics service has already been shutdown");
        try {
            ThreadedQueuedMetricsCollectionEvent event = new ThreadedQueuedMetricsCollectionEvent.Shutdown(initiatedEvents.get());
            queueEventSubmitter.submitEventToQueue(event);
            threadedQueuedConcurrentMetricsServiceThread.join(SHUTDOWN_WAIT_TIMEOUT_AS_MILLI);
        } catch (InterruptedException e) {
            String errMsg = String.format("Thread was interrupted while waiting for %s to complete",
                    threadedQueuedConcurrentMetricsServiceThread.getClass().getSimpleName());
            throw new MetricsCollectionException(errMsg, e);
        }
        shutdown.set(true);
    }

    public static class MetricsWorkloadResultFuture implements Future<WorkloadResultsSnapshot> {
        private final TimeSource timeSource;
        private final AtomicBoolean done = new AtomicBoolean(false);
        private final AtomicReference<WorkloadResultsSnapshot> startTime = new AtomicReference<>(null);

        private MetricsWorkloadResultFuture(TimeSource timeSource) {
            this.timeSource = timeSource;
        }

        synchronized void set(WorkloadResultsSnapshot value) throws MetricsCollectionException {
            if (done.get())
                throw new MetricsCollectionException("Value has already been set");
            startTime.set(value);
            done.set(true);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return done.get();
        }

        @Override
        public WorkloadResultsSnapshot get() {
            while (done.get() == false) {
                // wait for value to be set
            }
            return startTime.get();
        }

        @Override
        public WorkloadResultsSnapshot get(long timeout, TimeUnit unit) throws TimeoutException {
            long waitDurationMs = unit.toMillis(timeout);
            long startTimeMs = timeSource.nowAsMilli();
            while (timeSource.nowAsMilli() - startTimeMs < waitDurationMs) {
                // wait for value to be set
                if (done.get())
                    return startTime.get();
            }
            throw new TimeoutException("Could not complete future in time");
        }
    }

    public static class MetricsStatusFuture implements Future<WorkloadStatusSnapshot> {
        private final TimeSource timeSource;
        private final AtomicBoolean done = new AtomicBoolean(false);
        private final AtomicReference<WorkloadStatusSnapshot> status = new AtomicReference<>(null);

        private MetricsStatusFuture(TimeSource timeSource) {
            this.timeSource = timeSource;
        }

        synchronized void set(WorkloadStatusSnapshot value) throws MetricsCollectionException {
            if (done.get())
                throw new MetricsCollectionException("Value has already been set");
            status.set(value);
            done.set(true);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return done.get();
        }

        @Override
        public WorkloadStatusSnapshot get() {
            while (done.get() == false) {
                // wait for value to be set
            }
            return status.get();
        }

        @Override
        public WorkloadStatusSnapshot get(long timeout, TimeUnit unit) throws TimeoutException {
            long waitDurationMs = unit.toMillis(timeout);
            long startTimeMs = timeSource.nowAsMilli();
            while (timeSource.nowAsMilli() - startTimeMs < waitDurationMs) {
                // wait for value to be set
                if (done.get())
                    return status.get();
            }
            throw new TimeoutException("Could not complete future in time");
        }
    }
}
