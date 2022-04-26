package org.ldbcouncil.driver.control;

import org.ldbcouncil.driver.runtime.metrics.WorkloadResultsSnapshot;
import org.ldbcouncil.driver.runtime.metrics.WorkloadStatusSnapshot;

public interface LoggingService
{
    void info( String message );

    void status( WorkloadStatusSnapshot workloadStatusSnapshot,
            RecentThroughputAndDuration recentThroughputAndDuration,
            long completionTimeAsMilli );

    void summaryResult( WorkloadResultsSnapshot workloadResultsSnapshot );

    void detailedResult( WorkloadResultsSnapshot workloadResultsSnapshot );
}
