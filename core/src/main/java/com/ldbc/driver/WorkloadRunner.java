package com.ldbc.driver;

import org.apache.log4j.Logger;

import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorBuilder;
import com.ldbc.driver.measurements.WorkloadMetricsManager;
import com.ldbc.driver.util.temporal.Duration;
import com.ldbc.driver.util.temporal.Time;

public class WorkloadRunner
{
    private static Logger logger = Logger.getLogger( WorkloadRunner.class );

    private final Duration STATUS_INTERVAL = Duration.fromSeconds( 1 );

    private final Db db;
    private final BenchmarkPhase benchmarkPhase;
    private final Workload workload;
    private final int operationCount;
    private final GeneratorBuilder generatorBuilder;
    private final boolean showStatus;
    private final int threadCount;
    private final WorkloadMetricsManager metricsManager;

    int operationsDone;

    // TODO pass in Generator<Operation<<?>> instead of GeneratorBuilder?
    public WorkloadRunner( Db db, BenchmarkPhase benchmarkPhase, Workload workload, int operationCount,
            GeneratorBuilder generatorBuilder, boolean showStatus, int threadCount,
            WorkloadMetricsManager metricsManager )
    {
        this.db = db;
        this.benchmarkPhase = benchmarkPhase;
        this.workload = workload;
        this.operationCount = operationCount;
        this.operationsDone = 0;
        this.generatorBuilder = generatorBuilder;
        this.showStatus = showStatus;
        this.threadCount = threadCount;
        this.metricsManager = metricsManager;
    }

    public void run() throws WorkloadException
    {
        OperationHandlerExecutor operationHandlerExecutor = new OperationHandlerExecutor( threadCount );
        OperationResultLoggingThread operationResultLoggingThread = new OperationResultLoggingThread(
                operationHandlerExecutor, metricsManager );
        operationResultLoggingThread.start();
        Generator<Operation<?>> operationGenerator = getOperationGenerator( benchmarkPhase );
        WorkloadProgressStatus workloadProgressStatus = new WorkloadProgressStatus( Time.now() );
        while ( ( operationCount == -1 && operationGenerator.hasNext() ) || operationsDone < operationCount )
        {
            Operation<?> operation = operationGenerator.next();
            try
            {
                OperationHandler<?> operationHandler = db.getOperationHandler( operation );
                waitForScheduledStartTime( operation.getScheduledStartTime() );
                operationHandlerExecutor.execute( operationHandler );
                operationsDone++;
                if ( showStatus
                     && workloadProgressStatus.durationSinceLastUpdate().asSeconds() >= STATUS_INTERVAL.asSeconds() )
                {
                    String statusString = workloadProgressStatus.update( operationsDone );
                    logger.info( statusString );
                }
            }
            catch ( Exception e )
            {
                throw new WorkloadException( String.format(
                        "Error encountered trying to execute %s after %s of %s operations", operation, operationsDone,
                        operationCount ), e.getCause() );
            }
        }

        try
        {
            operationResultLoggingThread.finishLoggingRemainingResults();
            operationResultLoggingThread.join();
            operationHandlerExecutor.shutdown();
        }
        catch ( InterruptedException e )
        {
            logger.error( "Error encountered while waiting for logging thread to finish", e );
        }
    }

    private Generator<Operation<?>> getOperationGenerator( BenchmarkPhase benchmarkPhase ) throws WorkloadException
    {
        switch ( benchmarkPhase )
        {
        case LOAD_PHASE:
            return workload.getLoadOperations( generatorBuilder );
        case TRANSACTION_PHASE:
            return workload.getTransactionalOperations( generatorBuilder );
        }
        throw new WorkloadException( "Error encounterd trying to get operation generator" );
    }

    private void waitForScheduledStartTime( Time scheduledStartTime ) throws OperationException
    {
        if ( scheduledStartTime.equals( Operation.UNASSIGNED_SCHEDULED_START_TIME ) )
        {
            String errMsg = String.format( "Operation must have an assigned Scheduled Start Time" );
            logger.error( errMsg );
            throw new OperationException( errMsg );
        }
        while ( Time.now().asNano() < scheduledStartTime.asNano() )
        {
            // busy wait
        }
    }
}
