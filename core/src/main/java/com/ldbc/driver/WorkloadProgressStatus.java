package com.ldbc.driver;

import java.text.DecimalFormat;

public class WorkloadProgressStatus
{
    private long startTime;
    private long lastUpdateTime;

    private long totalOperations;

    public WorkloadProgressStatus( long startTime )
    {
        this.startTime = startTime;
        this.lastUpdateTime = this.startTime;
        this.totalOperations = 0;
    }

    public String update( long totalOperations )
    {
        lastUpdateTime = nowNanoSeconds();
        this.totalOperations = totalOperations;
        return statusString( lastUpdateTime );
    }

    public long secondsSinceLastUpdate()
    {
        return nanoSecondsToSeconds( nanoSecondsSinceLastUpdate() );
    }

    public long nanoSecondsSinceLastUpdate()
    {
        return nowNanoSeconds() - lastUpdateTime;
    }

    public String getStatus()
    {
        return statusString( nowNanoSeconds() );
    }

    private String statusString( long atTimeNanoSeconds )
    {
        DecimalFormat throughputFormat = new DecimalFormat( "#.00" );
        return String.format( "Status: Runtime (sec) [%s], Operations [%s], Throughput (op/sec) [%s]",
                nanoSecondsToSeconds( getElapsedTime( atTimeNanoSeconds ) ), totalOperations,
                throughputFormat.format( getThroughput( atTimeNanoSeconds ) ) );
    }

    private double getThroughput( long atTimeNanoSeconds )
    {
        return (double) totalOperations / nanoSecondsToSeconds( getElapsedTime( atTimeNanoSeconds ) );
    }

    private long getElapsedTime( long atTimeNanoSeconds )
    {
        return atTimeNanoSeconds - startTime;
    }

    private long nowNanoSeconds()
    {
        return System.nanoTime();
    }

    private long nanoSecondsToSeconds( long nanoSeconds )
    {
        return nanoSeconds / 1000000000;
    }
}
