package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

// TODO test
public class CompletionTimeValidator {
    private final ConcurrentCompletionTimeService completionTimeService;
    private final Duration gctDeltaTime;

    public CompletionTimeValidator(ConcurrentCompletionTimeService completionTimeService, Duration gctDeltaTime) {
        this.completionTimeService = completionTimeService;
        this.gctDeltaTime = gctDeltaTime;
    }

    public boolean gctIsReadyFor(Time time) throws CompletionTimeException {
        return completionTimeService.globalCompletionTime().plus(gctDeltaTime).gt(time);
    }
}
