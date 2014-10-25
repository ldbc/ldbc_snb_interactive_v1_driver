package com.ldbc.driver.runtime.coordination;

public class DummyLocalCompletionTimeWriter implements LocalCompletionTimeWriter {
    @Override
    public void submitLocalInitiatedTime(long timeAsMilli) throws CompletionTimeException {
        // do nothing
    }

    @Override
    public void submitLocalCompletedTime(long timeAsMilli) throws CompletionTimeException {
        // do nothing
    }
}
