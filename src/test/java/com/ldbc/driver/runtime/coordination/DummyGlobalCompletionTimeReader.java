package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.GlobalCompletionTimeReader;
import com.ldbc.driver.temporal.Time;

public class DummyGlobalCompletionTimeReader implements GlobalCompletionTimeReader {
    Time globalCompletionTime = null;

    public void setGlobalCompletionTime(Time globalCompletionTime) {
        this.globalCompletionTime = globalCompletionTime;
    }

    @Override
    public Time globalCompletionTime() throws CompletionTimeException {
        return globalCompletionTime;
    }
}
