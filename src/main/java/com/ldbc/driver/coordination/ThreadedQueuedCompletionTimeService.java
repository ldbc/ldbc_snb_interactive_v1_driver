package com.ldbc.driver.coordination;

import com.ldbc.driver.runner.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ThreadedQueuedCompletionTimeService implements CompletionTimeService {
    private static final Duration SHUTDOWN_WAIT_TIMEOUT = Duration.fromSeconds(5);

    private final Queue<CompletionTimeEvent> completionTimeEventQueue;
    private final AtomicReference<Time> gct;
    private final AtomicLong initiatedEvents;
    private final CompletionTimeMaintenanceThread completionTimeMaintenanceThread;
    private boolean shuttingDown = false;

    // TODO add support for setting initial GCT value
    public ThreadedQueuedCompletionTimeService(List<String> peerIds, ConcurrentErrorReporter errorReporter)
            throws CompletionTimeException {
        this.completionTimeEventQueue = new ConcurrentLinkedQueue<CompletionTimeEvent>();
        this.gct = new AtomicReference<Time>(null);
        this.initiatedEvents = new AtomicLong(0);
        LocalCompletionTime localCompletionTime = new LocalCompletionTime();
        ExternalCompletionTime externalCompletionTime = new ExternalCompletionTime(peerIds);
        completionTimeMaintenanceThread = new CompletionTimeMaintenanceThread(
                completionTimeEventQueue,
                errorReporter,
                localCompletionTime,
                externalCompletionTime,
                gct);
        completionTimeMaintenanceThread.start();
        try {
            if (null != globalCompletionTimeFuture().get())
                throw new CompletionTimeException(
                        String.format("Unexpected GCT while %s initialized", completionTimeMaintenanceThread.getClass().getSimpleName()));
        } catch (InterruptedException e) {
            throw new CompletionTimeException(
                    String.format("Thread interrupted while waiting for %s to initialize", completionTimeMaintenanceThread.getClass().getSimpleName()),
                    e.getCause());
        } catch (ExecutionException e) {
            throw new CompletionTimeException(
                    String.format("Error while waiting for %s to initialize", completionTimeMaintenanceThread.getClass().getSimpleName()),
                    e.getCause());
        }
    }

    @Override
    public Time globalCompletionTime() {
        return gct.get();
    }

    @Override
    synchronized public Future<Time> globalCompletionTimeFuture() throws CompletionTimeException {
        try {
            GlobalCompletionTimeFuture gctFuture = new GlobalCompletionTimeFuture();
            completionTimeEventQueue.add(CompletionTimeEvent.future(gctFuture));
            return gctFuture;
        } catch (Exception e) {
            String errMsg = String.format("Error requesting GCT future");
            throw new CompletionTimeException(errMsg, e.getCause());
        }
    }

    @Override
    synchronized public void submitInitiatedTime(Time time) throws CompletionTimeException {
        try {
            initiatedEvents.incrementAndGet();
            completionTimeEventQueue.add(CompletionTimeEvent.initiated(time));
        } catch (Exception e) {
            String errMsg = String.format("Error submitting initiated time for Time[%s]", time.toString());
            throw new CompletionTimeException(errMsg, e.getCause());
        }
    }

    @Override
    synchronized public void submitCompletedTime(Time time) throws CompletionTimeException {
        try {
            completionTimeEventQueue.add(CompletionTimeEvent.completed(time));
        } catch (Exception e) {
            String errMsg = String.format("Error submitting completed time for Time[%s]", time.toString());
            throw new CompletionTimeException(errMsg, e.getCause());
        }
    }

    @Override
    synchronized public void submitExternalCompletionTime(String peerId, Time time) throws CompletionTimeException {
        try {
            completionTimeEventQueue.add(CompletionTimeEvent.external(peerId, time));
        } catch (Exception e) {
            String errMsg = String.format("Error submitting external completion time for PeerID[%s] Time[%s]", peerId, time.toString());
            throw new CompletionTimeException(errMsg, e.getCause());
        }
    }

    @Override
    synchronized public void shutdown() throws CompletionTimeException {
        if (shuttingDown)
            return;
        shuttingDown = true;
        completionTimeEventQueue.add(CompletionTimeEvent.terminate(initiatedEvents.get()));
        try {
            completionTimeMaintenanceThread.join(SHUTDOWN_WAIT_TIMEOUT.asMilli());
        } catch (InterruptedException e) {
            String errMsg = String.format("Thread was interrupted while waiting for %s to complete",
                    completionTimeMaintenanceThread.getClass().getSimpleName());
            throw new CompletionTimeException(errMsg, e.getCause());
        }
    }

    public static class GlobalCompletionTimeFuture implements Future<Time> {
        private final AtomicBoolean done = new AtomicBoolean(false);
        private final AtomicReference<Time> globalCompletionTimeValue = new AtomicReference<Time>(null);

        synchronized void set(Time value) throws CompletionTimeException {
            if (done.get())
                throw new CompletionTimeException("Value has already been set");
            this.globalCompletionTimeValue.set(value);
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
        public Time get() {
            while (done.get() == false) {
                // wait for value to be set
            }
            return globalCompletionTimeValue.get();
        }

        @Override
        public Time get(long timeout, TimeUnit unit) throws TimeoutException {
            // Note: the commented version is cleaner, but .durationUntilNow() produces many Duration instances
            // Duration waitDuration = Duration.from(unit, timeout);
            // DurationMeasurement durationWaited = DurationMeasurement.startMeasurementNow();
            // while (durationWaited.durationUntilNow().lessThan(waitDuration)) {}
            long waitDurationMs = Duration.from(unit, timeout).asMilli();
            long startTimeMs = Time.nowAsMilli();
            while (Time.nowAsMilli() - startTimeMs < waitDurationMs) {
                // wait for value to be set
                if (done.get())
                    return globalCompletionTimeValue.get();
            }
            throw new TimeoutException("Could not complete future in time");
        }
    }
}
