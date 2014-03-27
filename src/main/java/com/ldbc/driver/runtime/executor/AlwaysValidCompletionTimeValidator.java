package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.temporal.Time;

public class AlwaysValidCompletionTimeValidator implements CompletionTimeValidator {
    @Override
    public boolean isValid(ConcurrentCompletionTimeService completionTimeService, Time time) {
        return true;
    }
}
