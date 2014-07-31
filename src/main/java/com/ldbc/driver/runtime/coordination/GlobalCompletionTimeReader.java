package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.temporal.Time;

public interface GlobalCompletionTimeReader {
    Time globalCompletionTime() throws CompletionTimeException;
}
