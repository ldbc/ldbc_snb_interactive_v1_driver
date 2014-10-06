package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.DefaultQueues;
import com.ldbc.driver.runtime.QueueEventSubmitter;
import com.ldbc.driver.runtime.scheduling.ExecutionDelayPolicy;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.CsvFileWriter;

import java.io.File;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ThreadedQueuedConcurrentMetricsService implements ConcurrentMetricsService {
    private static final Duration SHUTDOWN_WAIT_TIMEOUT = Duration.fromSeconds(5);
    private static final Duration FUTURE_GET_TIMEOUT = Duration.fromSeconds(5);

    public static final String RESULTS_LOG_FILENAME_SUFFIX = "-results_log.csv";
    public static final String RESULTS_METRICS_FILENAME_SUFFIX = "-results.json";
    public static final String RESULTS_CONFIGURATION_FILENAME_SUFFIX = "-configuration.properties";

    // TODO this could come from config, if we had a max_runtime parameter. for now, it can default to something
    public static final Duration DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION = Duration.fromMinutes(30);
    public static final Duration DEFAULT_HIGHEST_EXPECTED_DELAY_DURATION = Duration.fromMinutes(60);

    private final TimeSource timeSource;
    private final QueueEventSubmitter<MetricsCollectionEvent> queueEventSubmitter;
    private final AtomicLong initiatedEvents;
    private final ThreadedQueuedConcurrentMetricsServiceThread threadedQueuedConcurrentMetricsServiceThread;
    private AtomicBoolean shutdown = new AtomicBoolean(false);

    public static ThreadedQueuedConcurrentMetricsService newInstanceUsingNonBlockingQueue(TimeSource timeSource,
                                                                                          ConcurrentErrorReporter errorReporter,
                                                                                          TimeUnit unit,
                                                                                          Time initialTime,
                                                                                          Duration maxRuntimeDuration,
                                                                                          boolean recordStartTimeDelayLatency,
                                                                                          ExecutionDelayPolicy executionDelayPolicy,
                                                                                          CsvFileWriter csvResultsLogWriter) {
        Queue<MetricsCollectionEvent> queue = DefaultQueues.newNonBlocking();
        return new ThreadedQueuedConcurrentMetricsService(
                timeSource,
                errorReporter,
                unit,
                initialTime,
                maxRuntimeDuration,
                queue,
                recordStartTimeDelayLatency,
                executionDelayPolicy,
                csvResultsLogWriter);
    }

    public static ThreadedQueuedConcurrentMetricsService newInstanceUsingBlockingQueue(TimeSource timeSource,
                                                                                       ConcurrentErrorReporter errorReporter,
                                                                                       TimeUnit unit,
                                                                                       Time initialTime,
                                                                                       Duration maxRuntimeDuration,
                                                                                       boolean recordStartTimeDelayLatency,
                                                                                       ExecutionDelayPolicy executionDelayPolicy,
                                                                                       CsvFileWriter csvResultsLogWriter) {
        Queue<MetricsCollectionEvent> queue = DefaultQueues.newBlockingUnbounded();
        return new ThreadedQueuedConcurrentMetricsService(
                timeSource,
                errorReporter,
                unit,
                initialTime,
                maxRuntimeDuration,
                queue,
                recordStartTimeDelayLatency,
                executionDelayPolicy,
                csvResultsLogWriter);
    }

    private ThreadedQueuedConcurrentMetricsService(TimeSource timeSource,
                                                   ConcurrentErrorReporter errorReporter,
                                                   TimeUnit unit,
                                                   Time initialTime,
                                                   Duration maxRuntimeDuration,
                                                   Queue<MetricsCollectionEvent> queue,
                                                   boolean recordStartTimeDelayLatency,
                                                   ExecutionDelayPolicy executionDelayPolicy,
                                                   CsvFileWriter csvResultsLogWriter) {
        this.timeSource = timeSource;
        this.queueEventSubmitter = QueueEventSubmitter.queueEventSubmitterFor(queue);
        this.initiatedEvents = new AtomicLong(0);
        threadedQueuedConcurrentMetricsServiceThread = new ThreadedQueuedConcurrentMetricsServiceThread(
                errorReporter,
                queue,
                new MetricsManager(timeSource, unit, initialTime, maxRuntimeDuration, executionDelayPolicy.toleratedDelay(), recordStartTimeDelayLatency),
                recordStartTimeDelayLatency,
                executionDelayPolicy,
                csvResultsLogWriter);
        threadedQueuedConcurrentMetricsServiceThread.start();
    }

    @Override
    public void submitOperationResult(OperationResultReport operationResultReport) throws MetricsCollectionException {
        if (shutdown.get()) {
            throw new MetricsCollectionException("Can not submit a result after calling shutdown");
        }
        try {
            initiatedEvents.incrementAndGet();
            queueEventSubmitter.submitEventToQueue(MetricsCollectionEvent.submitResult(operationResultReport));
        } catch (InterruptedException e) {
            String errMsg = String.format("Error submitting result [%s]", operationResultReport.toString());
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
            queueEventSubmitter.submitEventToQueue(MetricsCollectionEvent.status(statusFuture));
            return statusFuture.get(FUTURE_GET_TIMEOUT.asMilli(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new MetricsCollectionException("Error while submitting request for workload status", e);
        } catch (TimeoutException e) {
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
            queueEventSubmitter.submitEventToQueue(MetricsCollectionEvent.workloadResult(workloadResultFuture));
            return workloadResultFuture.get(FUTURE_GET_TIMEOUT.asMilli(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new MetricsCollectionException("Error while submitting request for workload results", e);
        } catch (TimeoutException e) {
            throw new MetricsCollectionException("Error while submitting request for workload results", e);
        }
    }

    @Override
    synchronized public void shutdown() throws MetricsCollectionException {
        if (shutdown.get())
            throw new MetricsCollectionException("Metrics service has already been shutdown");
        try {
            queueEventSubmitter.submitEventToQueue(MetricsCollectionEvent.terminate(initiatedEvents.get()));
            threadedQueuedConcurrentMetricsServiceThread.join(SHUTDOWN_WAIT_TIMEOUT.asMilli());
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
        private final AtomicReference<WorkloadResultsSnapshot> startTime = new AtomicReference<WorkloadResultsSnapshot>(null);

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
            // Note: the commented version is cleaner, but .durationUntilNow() produces many Duration instances
            // Duration waitDuration = Duration.from(unit, timeout);
            // DurationMeasurement durationWaited = DurationMeasurement.startMeasurementNow();
            // while (durationWaited.durationUntilNow().lessThan(waitDuration)) {}
            long waitDurationMs = Duration.from(unit, timeout).asMilli();
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
            // Note: the commented version is cleaner, but .durationUntilNow() produces many Duration instances
            // Duration waitDuration = Duration.from(unit, timeout);
            // DurationMeasurement durationWaited = DurationMeasurement.startMeasurementNow();
            // while (durationWaited.durationUntilNow().lessThan(waitDuration)) {}
            long waitDurationMs = Duration.from(unit, timeout).asMilli();
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
