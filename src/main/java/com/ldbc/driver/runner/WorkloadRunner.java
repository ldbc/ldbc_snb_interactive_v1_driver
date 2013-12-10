package com.ldbc.driver.runner;

import java.util.Iterator;

import org.apache.log4j.Logger;

import com.ldbc.driver.Db;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.metrics.WorkloadMetricsManager;
import com.ldbc.driver.util.temporal.Duration;
import com.ldbc.driver.util.temporal.Time;

public class WorkloadRunner
{
    private static Logger logger = Logger.getLogger( WorkloadRunner.class );

    private final Duration DEFAULT_STATUS_UPDATE_INTERVAL = Duration.fromSeconds( 2 );
    public static final Duration DEFAULT_TOLERATED_OPERATION_START_TIME_DELAY = Duration.fromSeconds( 1 );

    private final Db db;
    private final Spinner spinner;
    private final OperationHandlerExecutor operationHandlerExecutor;
    private final MetricsLoggingThread metricsLoggingThread;
    private final WorkloadStatusThread workloadStatusThread;
    private final Iterator<Operation<?>> operationGenerator;
    private final WorkloadMetricsManager metricsManager;
    private final boolean showStatus;

    public WorkloadRunner( Db db, Iterator<Operation<?>> operationGenerator, boolean showStatus, int threadCount,
            WorkloadMetricsManager metricsManager ) throws WorkloadException
    {
        this.db = db;
        this.spinner = new Spinner( new CompulsoryStartTimeOperationSchedulingPolicy(
                DEFAULT_TOLERATED_OPERATION_START_TIME_DELAY ) );
        this.operationHandlerExecutor = new ThreadPoolOperationHandlerExecutor( threadCount );
        this.metricsLoggingThread = new MetricsLoggingThread( operationHandlerExecutor, metricsManager );
        Duration statusInterval = DEFAULT_STATUS_UPDATE_INTERVAL;
        this.workloadStatusThread = new WorkloadStatusThread( statusInterval, metricsManager );
        this.operationGenerator = operationGenerator;
        this.metricsManager = metricsManager;
        this.showStatus = showStatus;
    }

    public void run() throws WorkloadException
    {
        metricsManager.setStartTime( Time.now() );
        metricsLoggingThread.start();
        if ( showStatus ) workloadStatusThread.start();
        while ( operationGenerator.hasNext() )
        {
            Operation<?> operation = operationGenerator.next();
            try
            {
                OperationHandler<?> operationHandler = db.getOperationHandler( operation );
                // TODO consider putting spinner IN handler to reduce delay
                spinner.waitForScheduledStartTime( operation );
                operationHandlerExecutor.execute( operationHandler );
            }
            catch ( Exception e )
            {
                throw new WorkloadException( String.format(
                        "Error encountered trying to schedule operation [%s] to execute after %s operations",
                        operation, metricsManager.getMeasurementCount() ), e.getCause() );
            }
        }

        try
        {
            metricsLoggingThread.finishLoggingRemainingResults();
            metricsLoggingThread.join();
            if ( showStatus ) workloadStatusThread.interrupt();
        }
        catch ( InterruptedException e )
        {
            logger.error( "Error encountered while waiting for logging thread to finish", e );
        }

        try
        {
            operationHandlerExecutor.shutdown();
        }
        catch ( OperationHandlerExecutorException e )
        {
            logger.error( "Error encountered while shutting down operation handler executor", e );
        }
    }
}
