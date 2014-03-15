package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.temporal.Time;

import java.util.concurrent.Future;

// TODO test

/**
 * An instance of this class is given to OperationHandlers that should not be changing Global Completion Time.
 * Some event/operation types depend on others (in time), but are not depended on.
 * The scheduled start time of these operations should not be written to Global Completion Time,
 * as it would result in operations unnecessarily advancing Global Completion Time,
 * making it more difficult for the driver to do things in parallel
 * (it would have less freedom in the way it schedules operations).
 */
public class ReadOnlyConcurrentCompletionTimeService implements ConcurrentCompletionTimeService {
    private final ConcurrentCompletionTimeService innerConcurrentCompletionTimeService;

    public ReadOnlyConcurrentCompletionTimeService(ConcurrentCompletionTimeService innerConcurrentCompletionTimeService) {
        this.innerConcurrentCompletionTimeService = innerConcurrentCompletionTimeService;
    }

    @Override
    public Time globalCompletionTime() throws CompletionTimeException {
        return innerConcurrentCompletionTimeService.globalCompletionTime();
    }

    @Override
    public Future<Time> globalCompletionTimeFuture() throws CompletionTimeException {
        return innerConcurrentCompletionTimeService.globalCompletionTimeFuture();
    }

    @Override
    public void submitInitiatedTime(Time time) throws CompletionTimeException {
        // do nothing
    }

    @Override
    public void submitCompletedTime(Time time) throws CompletionTimeException {
        // do nothing
    }

    @Override
    public void submitExternalCompletionTime(String peerId, Time time) throws CompletionTimeException {
        // do nothing
    }

    @Override
    public void shutdown() throws CompletionTimeException {
        innerConcurrentCompletionTimeService.shutdown();
    }
}
