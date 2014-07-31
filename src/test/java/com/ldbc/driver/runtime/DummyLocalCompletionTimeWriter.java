package com.ldbc.driver.runtime;

import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.temporal.Time;

public class DummyLocalCompletionTimeWriter implements LocalCompletionTimeWriter {
    @Override
    public void submitLocalInitiatedTime(Time time) throws CompletionTimeException {
        // do nothing
    }

    @Override
    public void submitLocalCompletedTime(Time time) throws CompletionTimeException {
        // do nothing
    }
}
