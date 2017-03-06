package com.ldbc.driver.runtime.metrics;

import java.io.IOException;

public interface ResultsLogWriter extends AutoCloseable
{
    void write(
            String operationName,
            long scheduledStartTimeAsMilli,
            long actualStartTimeAsMilli,
            long runDurationAsNano,
            int resultCode,
            long originalStartTime ) throws IOException;

}
