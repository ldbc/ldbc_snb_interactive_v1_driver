package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.temporal.Time;

import java.util.Set;

public interface ExternalCompletionTimeReader {
    Time externalCompletionTime() throws CompletionTimeException;
}
