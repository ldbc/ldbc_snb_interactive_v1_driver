package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.temporal.Time;

import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class NaiveSynchronizedConcurrentCompletionTimeService implements ConcurrentCompletionTimeService {
    private final GlobalCompletionTime gct;

    private enum OPERATION {
        READ_GCT,
        READ_FUTURE_GCT,
        WRITE_INITIATED,
        WRITE_COMPLETED,
        WRITE_EXTERNAL
    }

    public NaiveSynchronizedConcurrentCompletionTimeService(Set<String> peerIds) throws CompletionTimeException {
        LocalCompletionTime localCompletionTime = new LocalCompletionTime();
        ExternalCompletionTime externalCompletionTime = new ExternalCompletionTime(peerIds);
        this.gct = new GlobalCompletionTime(localCompletionTime, externalCompletionTime);
    }

    @Override
    public Time globalCompletionTime() throws CompletionTimeException {
        return doOperation(OPERATION.READ_GCT, null, null).get();
    }

    @Override
    public Future<Time> globalCompletionTimeFuture() throws CompletionTimeException {
        return doOperation(OPERATION.READ_FUTURE_GCT, null, null);
    }

    @Override
    public void submitInitiatedTime(Time time) throws CompletionTimeException {
        doOperation(OPERATION.WRITE_INITIATED, null, time);
    }

    @Override
    public void submitCompletedTime(Time time) throws CompletionTimeException {
        doOperation(OPERATION.WRITE_COMPLETED, null, time);
    }

    @Override
    public void submitExternalCompletionTime(String peerId, Time time) throws CompletionTimeException {
        doOperation(OPERATION.WRITE_EXTERNAL, peerId, time);
    }

    @Override
    public void shutdown() throws CompletionTimeException {
    }

    private GlobalCompletionTimeFuture doOperation(OPERATION operation, String peerId, Time time) throws CompletionTimeException {
        synchronized (gct) {
            switch (operation) {
                case READ_GCT:
                    return new GlobalCompletionTimeFuture(gct.completionTime());
                case READ_FUTURE_GCT:
                    return new GlobalCompletionTimeFuture(gct.completionTime());
                case WRITE_INITIATED:
                    gct.applyInitiatedTime(time);
                    return null;
                case WRITE_COMPLETED:
                    gct.applyCompletedTime(time);
                    return null;
                case WRITE_EXTERNAL:
                    gct.applyPeerCompletionTime(peerId, time);
                    return null;
                default:
                    throw new CompletionTimeException("This should never happen");
            }
        }
    }

    private static class GlobalCompletionTimeFuture implements Future<Time> {
        private final Time globalCompletionTimeValue;

        public GlobalCompletionTimeFuture(Time globalCompletionTimeValue) {
            this.globalCompletionTimeValue = globalCompletionTimeValue;
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
            return true;
        }

        @Override
        public Time get() {
            return globalCompletionTimeValue;
        }

        @Override
        public Time get(long timeout, TimeUnit unit) {
            return globalCompletionTimeValue;
        }
    }
}
