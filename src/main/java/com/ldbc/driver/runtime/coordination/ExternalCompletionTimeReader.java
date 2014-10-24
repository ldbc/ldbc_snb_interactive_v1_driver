package com.ldbc.driver.runtime.coordination;

public interface ExternalCompletionTimeReader {
    long externalCompletionTimeAsMilli() throws CompletionTimeException;
}
