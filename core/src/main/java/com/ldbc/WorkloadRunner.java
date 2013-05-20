package com.ldbc;

import com.ldbc.db.Db;
import com.ldbc.db.DbException;
import com.ldbc.generator.Generator;
import com.ldbc.generator.GeneratorBuilder;
import com.ldbc.workloads.Workload;
import com.ldbc.workloads.WorkloadException;

class WorkloadRunner
{
    private final Db db;
    private final BenchmarkPhase benchmarkPhase;
    private final Workload workload;
    private final int operationCount;
    private final double targetPerformancePerMs;
    private final GeneratorBuilder generatorBuilder;
    private final boolean showStatus;

    int operationsDone;

    public WorkloadRunner( Db db, BenchmarkPhase benchmarkPhase, Workload workload, int operationCount,
            double targetPerformancePerMs, GeneratorBuilder generatorBuilder, boolean showStatus )
    {
        this.db = db;
        this.benchmarkPhase = benchmarkPhase;
        this.workload = workload;
        this.operationCount = operationCount;
        this.operationsDone = 0;
        this.targetPerformancePerMs = targetPerformancePerMs;
        this.generatorBuilder = generatorBuilder;
        this.showStatus = showStatus;
    }

    public void run() throws ClientException
    {
        Generator<Operation<?>> operationGenerator = getOperationGenerator( benchmarkPhase );
        runBenchmark( operationGenerator );
    }

    private void runBenchmark( Generator<Operation<?>> operationGenerator ) throws ClientException
    {
        long startTime = System.currentTimeMillis();
        while ( operationCount == 0 || operationsDone < operationCount )
        {
            Operation<?> operation = operationGenerator.next();
            try
            {
                OperationHandler<?> operationHandler = db.getOperationHandler( operation );
                // TODO perform asynchronously
                operationHandler.execute( operation );
                operationsDone++;
                doThrottleOperations( startTime );

                if ( showStatus )
                {
                    // TODO
                    // boolean standardstatus = false;
                    // if ( MapUtils.mapGetDefault( commandlineProperties,
                    // "measurementtype", "" ).equals( "timeseries" ) )
                    // {
                    // standardstatus = true;
                    // }
                    // statusThread = new StatusThread( clientThreads, argLabel,
                    // standardstatus );
                    // statusThread.start();
                }
            }
            catch ( DbException e )
            {
                throw new ClientException( String.format(
                        "Error encountered trying to execute %s after %s of %s operations", operation, operationsDone,
                        operationCount ), e.getCause() );
            }
        }
    }

    private Generator<Operation<?>> getOperationGenerator( BenchmarkPhase benchmarkPhase ) throws ClientException
    {
        Generator<Operation<?>> operationGenerator = null;
        try
        {
            switch ( benchmarkPhase )
            {
            case LOAD_PHASE:
                operationGenerator = workload.getLoadOperations( generatorBuilder );
                break;
            case TRANSACTION_PHASE:
                operationGenerator = workload.getTransactionalOperations( generatorBuilder );
                break;
            }
        }
        catch ( WorkloadException e )
        {
            throw new ClientException( "Error encounterd trying to get operation generator", e.getCause() );
        }
        return operationGenerator;
    }

    // TODO this seems super shit, what is it doing?
    // TODO probably needs removed and replaced with different abstraction
    private void doThrottleOperations( long startTime )
    {
        /*
         * more accurate than other strategies tried, like sleeping for (1/target)-operation_latency.
         * this way smoothes timing inaccuracies, (sleep() takes int, current time in millis) over many operations
         */
        if ( targetPerformancePerMs > 0 )
        {
            while ( System.currentTimeMillis() - startTime < ( (double) operationsDone ) / targetPerformancePerMs )
            {
                try
                {
                    Thread.sleep( 1 );
                }
                catch ( InterruptedException e )
                {
                    // do nothing.
                }
            }
        }
    }
}
