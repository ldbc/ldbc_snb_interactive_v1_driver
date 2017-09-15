package com.ldbc.driver.runtime.coordination;

import java.util.List;
import java.util.concurrent.Future;

public interface CompletionTimeService extends CompletionTimeReader
{
    CompletionTimeWriter newCompletionTimeWriter() throws CompletionTimeException;

    Future<Long> completionTimeAsMilliFuture() throws CompletionTimeException;

    List<CompletionTimeWriter> getAllWriters() throws CompletionTimeException;

    void shutdown() throws CompletionTimeException;
}
