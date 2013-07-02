package com.ldbc.driver;

import java.text.DecimalFormat;

import com.ldbc.driver.util.temporal.Duration;
import com.ldbc.driver.util.temporal.Time;

public class WorkloadProgressStatus
{
    private Time startTime;
    private Time lastUpdateTime;

    private long totalOperations;

    public WorkloadProgressStatus( Time startTime )
    {
        this.startTime = startTime;
        this.lastUpdateTime = Time.fromNano( this.startTime.asNano() );
        this.totalOperations = 0;
    }

    public String update( long totalOperations )
    {
        lastUpdateTime = Time.now();
        this.totalOperations = totalOperations;
        return statusString( lastUpdateTime );
    }

    public Duration durationSinceLastUpdate()
    {
        return Duration.durationBetween( lastUpdateTime, Time.now() );
    }

    public String getStatus()
    {
        return statusString( Time.now() );
    }

    private String statusString( Time atTime )
    {
        DecimalFormat throughputFormat = new DecimalFormat( "#.00" );
        return String.format( "Status: Runtime (sec) [%s], Operations [%s], Throughput (op/sec) [%s]",
                getElapsedTime( atTime ).asSeconds(), totalOperations,
                throughputFormat.format( getThroughput( atTime ) ) );
    }

    private double getThroughput( Time atTime )
    {
        return (double) totalOperations / getElapsedTime( atTime ).asSeconds();
    }

    private Duration getElapsedTime( Time atTime )
    {
        return Duration.durationBetween( startTime, atTime );
    }
}
