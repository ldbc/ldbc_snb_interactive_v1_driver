package com.ldbc.driver.runtime.coordination;

public class DummyGlobalCompletionTimeReader implements GlobalCompletionTimeReader {
    long globalCompletionTimeAsMilli = -1;

    public void setGlobalCompletionTimeAsMilli(long globalCompletionTimeAsMilli) {
        this.globalCompletionTimeAsMilli = globalCompletionTimeAsMilli;
    }

    @Override
    public long globalCompletionTimeAsMilli() throws CompletionTimeException {
        return globalCompletionTimeAsMilli;
    }
}
