package org.ldbcouncil.driver.control;

import org.ldbcouncil.driver.runtime.metrics.WorkloadResultsSnapshot;
import org.ldbcouncil.driver.runtime.metrics.WorkloadStatusSnapshot;

public class NullLoggingService implements LoggingService
{
    @Override
    public void info( String message )
    {
        // do nothing
    }

    @Override
    public void status(
            WorkloadStatusSnapshot workloadStatusSnapshot,
            RecentThroughputAndDuration recentThroughputAndDuration,
            long completionTimeAsMilli )
    {
        // do nothing
    }

    @Override
    public void summaryResult( WorkloadResultsSnapshot workloadResultsSnapshot )
    {
        // do nothing
    }

    @Override
    public void detailedResult( WorkloadResultsSnapshot workloadResultsSnapshot )
    {
        // do nothing
    }
}
