package com.ldbc;

import java.util.Map;

import org.apache.commons.math3.random.RandomDataGenerator;

/**
 * Workload generating thread
 * 
 * @author cooperb
 */
class ClientThread extends Thread
{
    final DB db;
    final BenchmarkPhase benchmarkPhase;
    final Workload workload;
    final int operationCount;
    final double targetPerformancePerMs;
    final int threadId;
    final int threadCount;

    // TODO remove now
    // final Properties properties;
    final Map<String, String> properties;

    final RandomDataGenerator random;

    // TODO can this be final too?
    // TODO it could if it were properly defined class, not Object
    Object workloadShareThreadState;

    int operationsDone;

    /**
     * @param db DB implementation to use
     * @param benchmarkPhase stage in benchmark execution (import/transactions)
     * @param workload workload definition
     * @param threadId id of this thread
     * @param threadCount total number of client threads
     * @param properties properties defining the benchmark
     * @param operationCount number of operations (read/write/update/etc.) to do
     * @param targetPerformancePerMs target operations-count per thread per ms
     */
    public ClientThread( DB db, BenchmarkPhase benchmarkPhase, Workload workload, int threadId, int threadCount,
            Map<String, String> properties, int operationCount, double targetPerformancePerMs,
            RandomDataGenerator randomDataGenerator )
    {
        // TODO: consider removing threadCount and threadId
        this.db = db;
        this.benchmarkPhase = benchmarkPhase;
        this.workload = workload;
        this.operationCount = operationCount;
        this.operationsDone = 0;
        this.targetPerformancePerMs = targetPerformancePerMs;
        this.threadId = threadId;
        this.threadCount = threadCount;
        this.properties = properties;
        this.random = randomDataGenerator;
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
            workloadShareThreadState = initBenchmark();
            randomizeClients();
            runBenchmark( benchmarkPhase );
            cleanupBenchmark();
        }
        catch ( ClientException e )
        {
            // TODO complete bullshit, fix error handling
            e.printStackTrace();
            e.printStackTrace( System.out );
            System.exit( 0 );
        }
    }

    // TODO WorkloadGenerator (control timing) could probably replace this
    // prevents clients from all sending requests at the same time
    // spread thread operations out so they do not all hit DB simultaneously
    private void randomizeClients()
    {
        try
        {
            // GH issue 4 - throws exception if target>1 because random.nextInt
            // argument must be >0
            // and sleep() doesn't make sense for granularities < 1 ms anyway
            if ( ( targetPerformancePerMs > 0 ) && ( targetPerformancePerMs <= 1.0 ) )
            {
                sleep( random.nextInt( 0, (int) ( 1.0 / targetPerformancePerMs ) ) );
            }
        }
        catch ( InterruptedException e )
        {
            // do nothing.
        }
    }

    private Object initBenchmark() throws ClientException
    {
        try
        {
            db.init();
        }
        catch ( DBException dbe )
        {
            throw new ClientException( "Error initializing database", dbe.getCause() );
        }

        try
        {
            return workload.initThread( properties, threadId, threadCount );
        }
        catch ( WorkloadException dbe )
        {
            throw new ClientException( "Error initializing thread-specific workload settings", dbe.getCause() );
        }

    }

    private void runBenchmark( BenchmarkPhase benchmarkPhase ) throws ClientException
    {
        long startTime = System.currentTimeMillis();
        while ( ( ( operationCount == 0 ) || ( operationsDone < operationCount ) ) && !workload.isStopRequested() )
        {
            try
            {
                if ( benchmarkPhase.equals( BenchmarkPhase.LOAD_PHASE ) )
                {
                    if ( false == workload.doInsert( db, workloadShareThreadState ) )
                    {
                        break;
                    }
                }
                else if ( benchmarkPhase.equals( BenchmarkPhase.TRANSACTION_PHASE ) )
                {
                    if ( false == workload.doTransaction( db, workloadShareThreadState ) )
                    {
                        break;
                    }
                }
            }
            catch ( WorkloadException e )
            {
                throw new ClientException( "Error encountered generating benchmark load", e.getCause() );
            }
            operationsDone++;
            doThrottleOperations( startTime );
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

    private void cleanupBenchmark() throws ClientException
    {
        try
        {
            db.cleanup();
        }
        catch ( DBException e )
        {
            throw new ClientException( "Error encountered during benchmark cleanup", e.getCause() );
        }
    }
}
