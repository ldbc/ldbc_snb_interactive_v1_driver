package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.OperationResult;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.metrics.formatters.OperationMetricsFormatter;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

// TODO test
public class ThreadedQueuedConcurrentMetricsService implements ConcurrentMetricsService {
    private static final Duration SHUTDOWN_WAIT_TIMEOUT = Duration.fromSeconds(5);

    private final Queue<MetricsCollectionEvent> metricsEventsQueue;
    private final AtomicLong initiatedEvents;
    private final ThreadedQueuedMetricsMaintenanceThread threadedQueuedMetricsMaintenanceThread;
    private boolean shuttingDown = false;

    public ThreadedQueuedConcurrentMetricsService(ConcurrentErrorReporter errorReporter, TimeUnit unit) {
        this.metricsEventsQueue = new ConcurrentLinkedQueue<MetricsCollectionEvent>();
        this.initiatedEvents = new AtomicLong(0);
        threadedQueuedMetricsMaintenanceThread = new ThreadedQueuedMetricsMaintenanceThread(errorReporter, metricsEventsQueue, new WorkloadMetricsManager(unit));
        threadedQueuedMetricsMaintenanceThread.start();
    }

    // TODO function to set start time? perhaps a "start measuring" function with Time as input?
    // TODO should not allow any other operations until that has been done, maybe thread should take care of that though to make calls here return faster

    @Override
    synchronized public void submitOperationResult(OperationResult operationResult) throws MetricsCollectionException {
        if (shuttingDown) {
            throw new MetricsCollectionException("Can not submit a result after calling shutdown");
        }
        try {
            initiatedEvents.incrementAndGet();
            metricsEventsQueue.add(MetricsCollectionEvent.result(operationResult));
        } catch (Exception e) {
            String errMsg = String.format("Error submitting result [%s]", operationResult.toString());
            throw new MetricsCollectionException(errMsg, e.getCause());
        }
    }

    @Override
    public void export(OperationMetricsFormatter metricsFormatter, OutputStream outputStream) throws MetricsCollectionException {
        MetricsExportFuture metricsExportFuture = new MetricsExportFuture(metricsFormatter);
        metricsEventsQueue.add(MetricsCollectionEvent.export(metricsExportFuture));
        String formattedMetrics = metricsExportFuture.get();
        try {
            // TODO specify charset... also this is a bit messy
            outputStream.write(formattedMetrics.getBytes());
        } catch (Exception e) {
            String errMsg = "Error encountered writing metrics to output stream";
            throw new MetricsCollectionException(errMsg, e.getCause());
        }
    }

    @Override
    public String status() throws MetricsCollectionException {
        MetricsStatusFuture metricsStatusFuture = new MetricsStatusFuture();
        metricsEventsQueue.add(MetricsCollectionEvent.status(metricsStatusFuture));
        return metricsStatusFuture.get();
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
            throw new MetricsCollectionException(errMsg, e.getCause());
        }
    }

    public static class MetricsExportFuture implements Future<String> {
        private final AtomicBoolean done = new AtomicBoolean(false);
        private final AtomicReference<String> formattedMetricsString = new AtomicReference<String>(null);
        private final ByteArrayOutputStream outputStream;
        private final PrintStream printStream;
        private final OperationMetricsFormatter metricsFormatter;

        public MetricsExportFuture(OperationMetricsFormatter metricsFormatter) {
            this.outputStream = new ByteArrayOutputStream();
            this.printStream = new PrintStream(outputStream);
            this.metricsFormatter = metricsFormatter;
        }

        synchronized void export(WorkloadMetricsManager metricsManager) throws MetricsCollectionException {
            if (done.get())
                throw new MetricsCollectionException("Value has already been set");
            try {
                metricsManager.export(metricsFormatter, printStream, Charset.forName("UTF-8"));
            } catch (MetricsCollectionException e) {
                throw new MetricsCollectionException("Encountered error while trying to export metrics", e.getCause());
            }
            formattedMetricsString.set(outputStream.toString());
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
        public String get() {
            while (done.get() == false) {
                // wait for value to be set
            }
            return formattedMetricsString.get();
        }

        @Override
        public String get(long timeout, TimeUnit unit) throws TimeoutException {
            // Note: the commented version is cleaner, but .durationUntilNow() produces many Duration instances
            // Duration waitDuration = Duration.from(unit, timeout);
            // DurationMeasurement durationWaited = DurationMeasurement.startMeasurementNow();
            // while (durationWaited.durationUntilNow().lessThan(waitDuration)) {}
            long waitDurationMs = Duration.from(unit, timeout).asMilli();
            long startTimeMs = Time.nowAsMilli();
            while (Time.nowAsMilli() - startTimeMs < waitDurationMs) {
                // wait for value to be set
                if (done.get())
                    return formattedMetricsString.get();
            }
            throw new TimeoutException("Could not complete future in time");
        }
    }

    public static class MetricsStatusFuture implements Future<String> {
        private final AtomicBoolean done = new AtomicBoolean(false);
        private final AtomicReference<String> statusString = new AtomicReference<String>(null);


        synchronized void set(String value) throws MetricsCollectionException {
            if (done.get())
                throw new MetricsCollectionException("Value has already been set");
            statusString.set(value);
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
        public String get() {
            while (done.get() == false) {
                // wait for value to be set
            }
            return statusString.get();
        }

        @Override
        public String get(long timeout, TimeUnit unit) throws TimeoutException {
            // Note: the commented version is cleaner, but .durationUntilNow() produces many Duration instances
            // Duration waitDuration = Duration.from(unit, timeout);
            // DurationMeasurement durationWaited = DurationMeasurement.startMeasurementNow();
            // while (durationWaited.durationUntilNow().lessThan(waitDuration)) {}
            long waitDurationMs = Duration.from(unit, timeout).asMilli();
            long startTimeMs = Time.nowAsMilli();
            while (Time.nowAsMilli() - startTimeMs < waitDurationMs) {
                // wait for value to be set
                if (done.get())
                    return statusString.get();
            }
            throw new TimeoutException("Could not complete future in time");
        }
    }
}
