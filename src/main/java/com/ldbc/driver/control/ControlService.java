package com.ldbc.driver.control;

import com.ldbc.driver.temporal.TimeSource;

public interface ControlService
{
    DriverConfiguration getConfiguration();

    LoggingServiceFactory getLoggingServiceFactory();

    TimeSource timeSource();

    void setWorkloadStartTimeAsMilli( long workloadStartTimeAsMilli );

    long workloadStartTimeAsMilli();

    void shutdown();
}
