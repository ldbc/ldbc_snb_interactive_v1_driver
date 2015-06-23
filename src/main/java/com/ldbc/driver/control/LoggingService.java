package com.ldbc.driver.control;

import com.ldbc.driver.runtime.metrics.WorkloadResultsSnapshot;
import com.ldbc.driver.runtime.metrics.WorkloadStatusSnapshot;

public interface LoggingService
{
    void info( String message );

    void status( WorkloadStatusSnapshot workloadStatusSnapshot,
            RecentThroughputAndDuration recentThroughputAndDuration,
            long globalCompletionTimeAsMilli );

    void summaryResult( WorkloadResultsSnapshot workloadResultsSnapshot );

    void detailedResult( WorkloadResultsSnapshot workloadResultsSnapshot );
}
