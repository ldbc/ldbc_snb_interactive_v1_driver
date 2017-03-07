package com.ldbc.driver.runtime.metrics;

import java.util.concurrent.TimeUnit;

public interface ResultsLogReader extends AutoCloseable
{
    boolean next();

    TimeUnit unit();

    String getOperationName();

    long getScheduledStartTimeAsMilli();

    long getActualStartTimeAsMilli();

    long getRunDurationAsNano();

    int getResultCode();

    long getOriginalStartTime();
}
