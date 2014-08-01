package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;

import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ThreadedQueuedConcurrentMetricsService implements ConcurrentMetricsService {
    private static final Duration SHUTDOWN_WAIT_TIMEOUT = Duration.fromSeconds(5);

    private final TimeSource TIME_SOURCE;
    private final QueueEventSubmitter queueEventSubmitter;
    private final AtomicLong initiatedEvents;
    private final ThreadedQueuedMetricsMaintenanceThread threadedQueuedMetricsMaintenanceThread;
    private AtomicBoolean shutdown = new AtomicBoolean(false);

    public static ThreadedQueuedConcurrentMetricsService newInstanceUsingNonBlockingQueue(TimeSource timeSource,
                                                                                          ConcurrentErrorReporter errorReporter,
                                                                                          TimeUnit unit,
                                                                                          Time initialTime) {
        Queue<MetricsCollectionEvent> queue = new ConcurrentLinkedQueue<>();
        return new ThreadedQueuedConcurrentMetricsService(timeSource, errorReporter, unit, initialTime, queue);
    }

    public static ThreadedQueuedConcurrentMetricsService newInstanceUsingBlockingQueue(TimeSource timeSource,
                                                                                       ConcurrentErrorReporter errorReporter,
                                                                                       TimeUnit unit,
                                                                                       Time initialTime) {
        Queue<MetricsCollectionEvent> queue = new LinkedBlockingQueue<>();
        return new ThreadedQueuedConcurrentMetricsService(timeSource, errorReporter, unit, initialTime, queue);
    }

    private ThreadedQueuedConcurrentMetricsService(TimeSource timeSource,
                                                   ConcurrentErrorReporter errorReporter,
                                                   TimeUnit unit,
                                                   Time initialTime,
                                                   Queue<MetricsCollectionEvent> queue) {
        this.TIME_SOURCE = timeSource;

        this.queueEventSubmitter = (BlockingQueue.class.isAssignableFrom(queue.getClass()))
                ? new BlockingQueueEventSubmitter((BlockingQueue) queue)
                : new NonBlockingQueueEventSubmitter(queue);

        this.initiatedEvents = new AtomicLong(0);
        threadedQueuedMetricsMaintenanceThread = new ThreadedQueuedMetricsMaintenanceThread(
                errorReporter,
                queue,
                new MetricsManager(TIME_SOURCE, unit, initialTime));
        threadedQueuedMetricsMaintenanceThread.start();
    }

    @Override
    synchronized public void submitOperationResult(OperationResultReport operationResultReport) throws MetricsCollectionException {
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
            MetricsStatusFuture statusFuture = new MetricsStatusFuture(TIME_SOURCE);
            queueEventSubmitter.submitEventToQueue(MetricsCollectionEvent.status(statusFuture));
            return statusFuture.get();
        } catch (InterruptedException e) {
            throw new MetricsCollectionException("Error while submitting request for workload status", e);
        }
    }

    @Override
    public WorkloadResultsSnapshot results() throws MetricsCollectionException {
        if (shutdown.get()) {
            throw new MetricsCollectionException("Can not retrieve results after calling shutdown");
        }
        try {
            MetricsWorkloadResultFuture workloadResultFuture = new MetricsWorkloadResultFuture(TIME_SOURCE);
            queueEventSubmitter.submitEventToQueue(MetricsCollectionEvent.workloadResult(workloadResultFuture));
            return workloadResultFuture.get();
        } catch (InterruptedException e) {
            throw new MetricsCollectionException("Error while submitting request for workload results", e);
        }
    }

    @Override
    synchronized public void shutdown() throws MetricsCollectionException {
        if (shutdown.get())
            throw new MetricsCollectionException("Metrics service has already been shutdown");
        try {
            queueEventSubmitter.submitEventToQueue(MetricsCollectionEvent.terminate(initiatedEvents.get()));
            threadedQueuedMetricsMaintenanceThread.join(SHUTDOWN_WAIT_TIMEOUT.asMilli());
        } catch (InterruptedException e) {
            String errMsg = String.format("Thread was interrupted while waiting for %s to complete",
                    threadedQueuedMetricsMaintenanceThread.getClass().getSimpleName());
            throw new MetricsCollectionException(errMsg, e);
        }
        shutdown.set(true);
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
        private final AtomicReference<WorkloadStatusSnapshot> status = new AtomicReference<>(null);

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

    private static class NonBlockingQueueEventSubmitter implements QueueEventSubmitter {
        private final Queue<MetricsCollectionEvent> queue;

        private NonBlockingQueueEventSubmitter(Queue<MetricsCollectionEvent> queue) {
            this.queue = queue;
        }

        @Override
        public void submitEventToQueue(MetricsCollectionEvent event) throws InterruptedException {
            queue.add(event);
        }
    }

    private static class BlockingQueueEventSubmitter implements QueueEventSubmitter {
        private final BlockingQueue<MetricsCollectionEvent> queue;

        private BlockingQueueEventSubmitter(BlockingQueue<MetricsCollectionEvent> queue) {
            this.queue = queue;
        }

        @Override
        public void submitEventToQueue(MetricsCollectionEvent event) throws InterruptedException {
            queue.put(event);
        }
    }

    private static interface QueueEventSubmitter {
        void submitEventToQueue(MetricsCollectionEvent event) throws InterruptedException;
    }
}
