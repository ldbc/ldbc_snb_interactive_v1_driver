package com.ldbc.driver.control;

import com.ldbc.driver.temporal.TimeSource;

public interface ControlService
{
    DriverConfiguration configuration();

    LoggingServiceFactory loggingServiceFactory();

    TimeSource timeSource();

    void setWorkloadStartTimeAsMilli( long workloadStartTimeAsMilli );

    long workloadStartTimeAsMilli();

    void waitForCommandToExecuteWorkload();

    void waitForAllToCompleteExecutingWorkload();

    void shutdown();
}
