package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.temporal.Time;

public interface CompletionTimeValidator {
    boolean isValid(ConcurrentCompletionTimeService completionTimeService, Time time) throws CompletionTimeException;
}
