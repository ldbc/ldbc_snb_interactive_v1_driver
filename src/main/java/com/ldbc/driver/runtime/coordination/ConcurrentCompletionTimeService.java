package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.temporal.Time;

import java.util.List;
import java.util.concurrent.Future;

public interface ConcurrentCompletionTimeService extends
        ExternalCompletionTimeWriter,
        GlobalCompletionTimeReader {

    LocalCompletionTimeWriter newLocalCompletionTimeWriter() throws CompletionTimeException;

    Future<Time> globalCompletionTimeFuture() throws CompletionTimeException;

    List<LocalCompletionTimeWriter> getAllWriters() throws CompletionTimeException;

    void shutdown() throws CompletionTimeException;
}