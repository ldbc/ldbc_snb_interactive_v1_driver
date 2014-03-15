package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.temporal.Time;

import java.util.concurrent.Future;

public interface ConcurrentCompletionTimeService {
    Time globalCompletionTime() throws CompletionTimeException;

    Future<Time> globalCompletionTimeFuture() throws CompletionTimeException;

    void submitInitiatedTime(Time time) throws CompletionTimeException;

    void submitCompletedTime(Time time) throws CompletionTimeException;

    void submitExternalCompletionTime(String peerId, Time time) throws CompletionTimeException;

    void shutdown() throws CompletionTimeException;
}
