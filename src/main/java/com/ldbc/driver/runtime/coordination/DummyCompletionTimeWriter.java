package com.ldbc.driver.runtime.coordination;

public class DummyCompletionTimeWriter implements CompletionTimeWriter
{
    @Override
    public void submitInitiatedTime(long timeAsMilli) throws CompletionTimeException {
        // do nothing
    }

    @Override
    public void submitCompletedTime(long timeAsMilli) throws CompletionTimeException {
        // do nothing
    }
}
