package com.ldbc.driver.runtime.coordination;

public interface CompletionTimeReader
{
    long lastKnownLowestInitiatedTimeAsMilli() throws CompletionTimeException;

    long completionTimeAsMilli() throws CompletionTimeException;
}
