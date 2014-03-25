package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

// TODO test
public class DeltaTimeCompletionTimeValidator implements CompletionTimeValidator {
    private final Duration gctDeltaTime;

    DeltaTimeCompletionTimeValidator(Duration gctDeltaTime) {
        this.gctDeltaTime = gctDeltaTime;
    }

    @Override
    public boolean isValid(ConcurrentCompletionTimeService completionTimeService, Time time) throws CompletionTimeException {
        return completionTimeService.globalCompletionTime().plus(gctDeltaTime).gt(time);
    }
}
