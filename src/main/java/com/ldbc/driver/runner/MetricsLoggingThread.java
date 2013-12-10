package com.ldbc.driver.runner;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.ldbc.driver.OperationResult;
import com.ldbc.driver.metrics.MetricException;
import com.ldbc.driver.metrics.WorkloadMetricsManager;

class MetricsLoggingThread extends Thread
{
    private static Logger logger = Logger.getLogger( MetricsLoggingThread.class );

    private final WorkloadMetricsManager metricsManager;

    private final OperationHandlerExecutor operationHandlerExecutor;
    private AtomicBoolean isMoreResultsComing = new AtomicBoolean( true );

    MetricsLoggingThread( OperationHandlerExecutor operationHandlerExecutor, WorkloadMetricsManager metricsManager )
    {
        this.operationHandlerExecutor = operationHandlerExecutor;
        this.metricsManager = metricsManager;
    }

    final void finishLoggingRemainingResults()
    {
        isMoreResultsComing.set( false );
    }

    @Override
    public void run()
    {
        try
        {
            // Log results
            while ( isMoreResultsComing.get() )
            {
                OperationResult operationResult = operationHandlerExecutor.nextOperationResultNonBlocking();
                if ( null == operationResult ) continue;
                log( operationResult );
            }
            // Log remaining results
            while ( true )
            {
                OperationResult operationResult = operationHandlerExecutor.nextOperationResultBlocking();
                if ( null == operationResult ) break;
                log( operationResult );
            }
        }
        catch ( Exception e )
        {
            String errMsg = "Error encountered while logging results";
            logger.error( errMsg, e );
            throw new RuntimeException( errMsg, e.getCause() );
        }
    }

    private void log( OperationResult operationResult )
    {
        try
        {
            metricsManager.measure( operationResult );
        }
        catch ( MetricException e )
        {
            String errMsg = String.format( "Error encountered while logging result - %s", operationResult );
            throw new MetricException( errMsg, e.getCause() );
        }
    }
}
