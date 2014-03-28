package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.temporal.Time;

public interface CompletionTimeValidator {
    boolean gctIsReadyFor(Time time) throws CompletionTimeException;
}
