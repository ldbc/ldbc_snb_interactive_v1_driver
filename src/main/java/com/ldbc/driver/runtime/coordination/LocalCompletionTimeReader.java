package com.ldbc.driver.runtime.coordination;

public interface LocalCompletionTimeReader {
    long lastKnownLowestInitiatedTimeAsMilli() throws CompletionTimeException;

    long localCompletionTimeAsMilli() throws CompletionTimeException;
}
