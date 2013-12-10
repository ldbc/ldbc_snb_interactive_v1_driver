package com.ldbc.driver.runner;

import org.apache.log4j.Logger;

import com.ldbc.driver.metrics.WorkloadMetricsManager;
import com.ldbc.driver.util.temporal.Duration;

class WorkloadStatusThread extends Thread
{
    private static Logger logger = Logger.getLogger( WorkloadStatusThread.class );

    private final Duration statusUpdateInterval;
    private final WorkloadMetricsManager metricsManager;

    WorkloadStatusThread( Duration statusUpdateInterval, WorkloadMetricsManager metricsManager )
    {
        this.statusUpdateInterval = statusUpdateInterval;
        this.metricsManager = metricsManager;
    }

    @Override
    public void run()
    {
        while ( true )
        {
            try
            {
                Thread.sleep( statusUpdateInterval.asMilli() );
                String statusString = metricsManager.getStatusString();
                logger.info( statusString );
            }
            catch ( InterruptedException e )
            {
                break;
            }
        }
    }
}
