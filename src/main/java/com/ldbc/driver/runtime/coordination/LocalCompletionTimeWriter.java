package com.ldbc.driver.runtime.coordination;

public interface LocalCompletionTimeWriter {
    void submitLocalInitiatedTime(long timeAsMilli) throws CompletionTimeException;

    void submitLocalCompletedTime(long timeAsMilli) throws CompletionTimeException;
}
