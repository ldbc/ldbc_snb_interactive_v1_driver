package org.ldbcouncil.driver.control;

import org.ldbcouncil.driver.temporal.TimeSource;

public interface ControlService
{
    DriverConfiguration configuration();

    LoggingServiceFactory loggingServiceFactory();

    TimeSource timeSource();

    void setWorkloadStartTimeAsMilli( long workloadStartTimeAsMilli );

    long workloadStartTimeAsMilli();

    void shutdown();
}
