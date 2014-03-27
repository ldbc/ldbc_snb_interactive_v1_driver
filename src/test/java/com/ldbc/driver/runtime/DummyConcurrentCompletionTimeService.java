package com.ldbc.driver.runtime;

import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.temporal.Time;

import java.util.concurrent.Future;

class DummyConcurrentCompletionTimeService implements ConcurrentCompletionTimeService {
    @Override
    public Time globalCompletionTime() throws CompletionTimeException {
        return null;
    }

    @Override
    public Future<Time> globalCompletionTimeFuture() throws CompletionTimeException {
        return null;
    }

    @Override
    public void submitInitiatedTime(Time time) throws CompletionTimeException {

    }

    @Override
    public void submitCompletedTime(Time time) throws CompletionTimeException {

    }

    @Override
    public void submitExternalCompletionTime(String peerId, Time time) throws CompletionTimeException {

    }

    @Override
    public void shutdown() throws CompletionTimeException {

    }
}
