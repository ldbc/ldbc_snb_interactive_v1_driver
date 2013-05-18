package com.ldbc2;

import java.util.Map;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.ldbc.db2.Db2;
import com.ldbc.db2.DbException2;
import com.ldbc.db2.Operation2;
import com.ldbc.db2.OperationHandler2;
import com.ldbc.generator.Generator;
import com.ldbc.generator.GeneratorBuilder;
import com.ldbc.workloads2.Workload2;
import com.ldbc.workloads2.WorkloadException2;

class ClientThread2 extends Thread
{
    final Db2 db;
    final BenchmarkPhase2 benchmarkPhase;
    final Workload2 workload;
    final int operationCount;
    final double targetPerformancePerMs;

    final Map<String, String> properties;
    final RandomDataGenerator random;
    final GeneratorBuilder generatorBuilder;

    int operationsDone;

    public ClientThread2( Db2 db, BenchmarkPhase2 benchmarkPhase, Workload2 workload, Map<String, String> properties,
            int operationCount, double targetPerformancePerMs, RandomDataGenerator random,
            GeneratorBuilder generatorBuilder )
    {
        this.db = db;
        this.benchmarkPhase = benchmarkPhase;
        this.workload = workload;
        this.operationCount = operationCount;
        this.operationsDone = 0;
        this.targetPerformancePerMs = targetPerformancePerMs;
        this.properties = properties;
        this.random = random;
        this.generatorBuilder = generatorBuilder;
    }

    public int getOpsDone()
    {
        return operationsDone;
    }

    @Override
    public void run()
    {
        try
        {
            db.init( properties );
        }
        catch ( DbException2 e )
        {
            throw new RuntimeException( "Error during database initialization", e.getCause() );
        }
        try
        {
            runBenchmark( benchmarkPhase );
        }
        catch ( ClientException2 e )
        {
            throw new RuntimeException( "Error running benchmark", e.getCause() );
        }
        try
        {
            db.cleanup();
        }
        catch ( DbException2 e )
        {
            throw new RuntimeException( "Error during benchmark cleanup", e.getCause() );
        }
    }

    private void runBenchmark( BenchmarkPhase2 benchmarkPhase ) throws ClientException2
    {
        Generator<Operation2<?>> operationGenerator = null;
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
        catch ( WorkloadException2 e )
        {
            throw new ClientException2( "Error encounterd trying to get operation generator", e.getCause() );
        }

        long startTime = System.currentTimeMillis();
        while ( operationCount == 0 || operationsDone < operationCount )
        {
            Operation2<?> operation = operationGenerator.next();
            try
            {
                OperationHandler2<?> operationHandler = db.getOperationHandler( operation );
                operationHandler.execute( operation );
                operationsDone++;
                doThrottleOperations( startTime );
            }
            catch ( DbException2 e )
            {
                throw new ClientException2( String.format( "Error encounterd trying to execute %s", operation ),
                        e.getCause() );
            }
        }
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
                    sleep( 1 );
                }
                catch ( InterruptedException e )
                {
                    // do nothing.
                }
            }
        }
    }
}
