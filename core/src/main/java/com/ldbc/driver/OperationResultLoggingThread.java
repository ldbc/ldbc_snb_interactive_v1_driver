package com.ldbc.driver;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.ldbc.driver.measurements.Measurements;

class OperationResultLoggingThread extends Thread
{
    private static Logger logger = Logger.getLogger( OperationResultLoggingThread.class );

    private final Measurements measurements;

    private final OperationHandlerExecutor operationHandlerExecutor;
    private AtomicBoolean isMoreResultsComing = new AtomicBoolean( true );

    OperationResultLoggingThread( OperationHandlerExecutor operationHandlerExecutor, Measurements measurements )
    {
        this.operationHandlerExecutor = operationHandlerExecutor;
        this.measurements = measurements;
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
                OperationResult operationResult = operationHandlerExecutor.nextOperationResult();
                if ( null == operationResult ) break;
                log( operationResult );
            }
        }
        catch ( Exception e )
        {
            logger.error( "Error encountered while logging results", e );
        }
    }

    private void log( OperationResult operationResult )
    {
        // TODO make NOT int
        measurements.measure( operationResult.getOperationType(), (int) operationResult.getRunTime() );
        measurements.reportReturnCode( operationResult.getOperationType(), operationResult.getResultCode() );
        // TODO log more stuff
        // operationResult.getScheduledStartTime()
        // operationResult.getActualStartTime()
        // operationResult.getResult()
    }
}
