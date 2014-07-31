package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.temporal.Time;

public interface LocalCompletionTimeWriter {
    void submitLocalInitiatedTime(Time time) throws CompletionTimeException;

    void submitLocalCompletedTime(Time time) throws CompletionTimeException;
}
