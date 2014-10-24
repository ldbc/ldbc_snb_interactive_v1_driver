package com.ldbc.driver.runtime.coordination;

public interface GlobalCompletionTimeReader {
    long globalCompletionTimeAsMilli() throws CompletionTimeException;
}
