package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.temporal.Time;

public class DummyLocalCompletionTimeWriter implements LocalCompletionTimeWriter {
    @Override
    public void submitLocalInitiatedTime(Time timeAsMilli) throws CompletionTimeException {
        // do nothing
    }

    @Override
    public void submitLocalCompletedTime(Time timeAsMilli) throws CompletionTimeException {
        // do nothing
    }
}
