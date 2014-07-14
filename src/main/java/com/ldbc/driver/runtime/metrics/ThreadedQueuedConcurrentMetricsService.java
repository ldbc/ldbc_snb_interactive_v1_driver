package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.TimeSource;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ThreadedQueuedConcurrentMetricsService implements ConcurrentMetricsService {
    private static final Duration SHUTDOWN_WAIT_TIMEOUT = Duration.fromSeconds(5);

    private final TimeSource TIME_SOURCE;
    private final Queue<MetricsCollectionEvent> metricsEventsQueue;
    private final AtomicLong initiatedEvents;
    private final ThreadedQueuedMetricsMaintenanceThread threadedQueuedMetricsMaintenanceThread;
    private boolean shuttingDown = false;

    public ThreadedQueuedConcurrentMetricsService(TimeSource timeSource, ConcurrentErrorReporter errorReporter, TimeUnit unit) {
        this.TIME_SOURCE = timeSource;
        this.metricsEventsQueue = new ConcurrentLinkedQueue<>();
        this.initiatedEvents = new AtomicLong(0);
        threadedQueuedMetricsMaintenanceThread = new ThreadedQueuedMetricsMaintenanceThread(
                errorReporter,
                metricsEventsQueue,
                new MetricsManager(TIME_SOURCE, unit));
        threadedQueuedMetricsMaintenanceThread.start();
    }

    @Override
    synchronized public void submitOperationResult(OperationResultReport operationResultReport) throws MetricsCollectionException {
        if (shuttingDown) {
            throw new MetricsCollectionException("Can not submit a result after calling shutdown");
        }
        try {
            initiatedEvents.incrementAndGet();
            metricsEventsQueue.add(MetricsCollectionEvent.submitResult(operationResultReport));
        } catch (Exception e) {
            String errMsg = String.format("Error submitting result [%s]", operationResultReport.toString());
            throw new MetricsCollectionException(errMsg, e);
        }
    }

    @Override
    public WorkloadStatusSnapshot status() throws MetricsCollectionException {
        if (shuttingDown) {
            throw new MetricsCollectionException("Can not read metrics status after calling shutdown");
        }
        MetricsStatusFuture statusFuture = new MetricsStatusFuture(TIME_SOURCE);
        metricsEventsQueue.add(MetricsCollectionEvent.status(statusFuture));
        return statusFuture.get();
    }

    @Override
    public WorkloadResultsSnapshot results() throws MetricsCollectionException {
        if (shuttingDown) {
            throw new MetricsCollectionException("Can not retrieve results after calling shutdown");
        }
        MetricsWorkloadResultFuture workloadResultFuture = new MetricsWorkloadResultFuture(TIME_SOURCE);
        metricsEventsQueue.add(MetricsCollectionEvent.workloadResult(workloadResultFuture));
        return workloadResultFuture.get();
    }

    @Override
    synchronized public void shutdown() throws MetricsCollectionException {
        if (shuttingDown)
            return;
        shuttingDown = true;
        metricsEventsQueue.add(MetricsCollectionEvent.terminate(initiatedEvents.get()));
        try {
            threadedQueuedMetricsMaintenanceThread.join(SHUTDOWN_WAIT_TIMEOUT.asMilli());
        } catch (InterruptedException e) {
            String errMsg = String.format("Thread was interrupted while waiting for %s to complete",
                    threadedQueuedMetricsMaintenanceThread.getClass().getSimpleName());
            throw new MetricsCollectionException(errMsg, e);
        }
    }

    public static class MetricsWorkloadResultFuture implements Future<WorkloadResultsSnapshot> {
        private final TimeSource TIME_SOURCE;
        private final AtomicBoolean done = new AtomicBoolean(false);
        private final AtomicReference<WorkloadResultsSnapshot> startTime = new AtomicReference<WorkloadResultsSnapshot>(null);

        private MetricsWorkloadResultFuture(TimeSource timeSource) {
            this.TIME_SOURCE = timeSource;
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
            long startTimeMs = TIME_SOURCE.nowAsMilli();
            while (TIME_SOURCE.nowAsMilli() - startTimeMs < waitDurationMs) {
                // wait for value to be set
                if (done.get())
                    return startTime.get();
            }
            throw new TimeoutException("Could not complete future in time");
        }
    }

    public static class MetricsStatusFuture implements Future<WorkloadStatusSnapshot> {
        private final TimeSource TIME_SOURCE;
        private final AtomicBoolean done = new AtomicBoolean(false);
        private final AtomicReference<WorkloadStatusSnapshot> status = new AtomicReference<WorkloadStatusSnapshot>(null);

        private MetricsStatusFuture(TimeSource timeSource) {
            this.TIME_SOURCE = timeSource;
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
            long startTimeMs = TIME_SOURCE.nowAsMilli();
            while (TIME_SOURCE.nowAsMilli() - startTimeMs < waitDurationMs) {
                // wait for value to be set
                if (done.get())
                    return status.get();
            }
            throw new TimeoutException("Could not complete future in time");
        }
    }
}
