package com.ldbc.driver;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.ldbc.driver.measurements.MetricException;
import com.ldbc.driver.measurements.WorkloadMetricsManager;

class OperationResultLoggingThread extends Thread
{
    private static Logger logger = Logger.getLogger( OperationResultLoggingThread.class );

    private final WorkloadMetricsManager metricsManager;

    private final OperationHandlerExecutor operationHandlerExecutor;
    private AtomicBoolean isMoreResultsComing = new AtomicBoolean( true );

    OperationResultLoggingThread( OperationHandlerExecutor operationHandlerExecutor,
            WorkloadMetricsManager metricsManager )
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
                OperationResult operationResult = operationHandlerExecutor.nextOperationResult();
                if ( null == operationResult ) continue;
                log( operationResult );
            }
            // Log remaining results
            while ( true )
            {
                OperationResult operationResult = operationHandlerExecutor.waitForNextOperationResult();
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
