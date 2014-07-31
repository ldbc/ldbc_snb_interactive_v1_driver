package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.temporal.Time;

public interface LocalCompletionTimeReader {
    Time lastKnownLowestInitiatedTime() throws CompletionTimeException;

    Time localCompletionTime() throws CompletionTimeException;
}
