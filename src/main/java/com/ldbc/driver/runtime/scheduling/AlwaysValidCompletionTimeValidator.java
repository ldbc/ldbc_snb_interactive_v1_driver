package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.temporal.Time;

public class AlwaysValidCompletionTimeValidator implements CompletionTimeValidator {
    @Override
    public boolean gctIsReadyFor(Time time) {
        return true;
    }
}
