package com.ldbc.driver.runtime.coordination;

import java.util.List;
import java.util.concurrent.Future;

public interface CompletionTimeService extends GlobalCompletionTimeReader
{
    LocalCompletionTimeWriter newLocalCompletionTimeWriter() throws CompletionTimeException;

    Future<Long> globalCompletionTimeAsMilliFuture() throws CompletionTimeException;

    List<LocalCompletionTimeWriter> getAllWriters() throws CompletionTimeException;

    void shutdown() throws CompletionTimeException;
}
