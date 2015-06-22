package com.ldbc.driver.control;

import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.temporal.TimeSource;

public class LocalControlService implements ControlService
{
    private final TemporalUtil temporalUtil = new TemporalUtil();
    private final DriverConfiguration configuration;
    private final LoggingServiceFactory loggingServiceFactory;
    private final TimeSource timeSource;
    private long workloadStartTimeAsMilli;

    public LocalControlService(
            long workloadStartTimeAsMilli,
            DriverConfiguration configuration,
            LoggingServiceFactory loggingServiceFactory,
            TimeSource timeSource )
    {
        this.workloadStartTimeAsMilli = workloadStartTimeAsMilli;
        this.configuration = configuration;
        this.loggingServiceFactory = loggingServiceFactory;
        this.timeSource = timeSource;
    }

    @Override
    public DriverConfiguration configuration()
    {
        return configuration;
    }

    @Override
    public LoggingServiceFactory loggingServiceFactory()
    {
        return loggingServiceFactory;
    }

    @Override
    public TimeSource timeSource()
    {
        return timeSource;
    }

    @Override
    public void setWorkloadStartTimeAsMilli( long workloadStartTimeAsMilli )
    {
        this.workloadStartTimeAsMilli = workloadStartTimeAsMilli;
    }

    @Override
    public long workloadStartTimeAsMilli()
    {
        return workloadStartTimeAsMilli;
    }

    @Override
    public void shutdown()
    {

    }

    @Override
    public String toString()
    {
        return "Workload Start Time:\t" + temporalUtil.milliTimeToDateTimeString( workloadStartTimeAsMilli ) + "\n" +
               configuration.toString();
    }
}
