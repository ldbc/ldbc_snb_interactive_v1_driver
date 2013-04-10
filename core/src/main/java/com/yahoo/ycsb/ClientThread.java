package com.yahoo.ycsb;

import java.util.Properties;

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
    final double target;
    final int threadId;
    final int threadCount;
    final Properties properties;
    final RandomDataGenerator random;

    // TODO can this be final too?
    Object workloadState;

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
            Properties properties, int operationCount, double targetPerformancePerMs,
            RandomDataGenerator randomDataGenerator )
    {
        // TODO: consider removing threadCount and threadId
        this.db = db;
        this.benchmarkPhase = benchmarkPhase;
        this.workload = workload;
        this.operationCount = operationCount;
        this.operationsDone = 0;
        this.target = targetPerformancePerMs;
        this.threadId = threadId;
        this.threadCount = threadCount;
        this.properties = properties;
        this.random = randomDataGenerator;
    }

    public int getOpsDone()
    {
        return operationsDone;
    }

    public void run()
    {
        try
        {
            db.init();
        }
        catch ( DBException e )
        {
            e.printStackTrace();
            e.printStackTrace( System.out );
            return;
        }

        try
        {
            workloadState = workload.initThread( properties, threadId, threadCount );
        }
        catch ( WorkloadException e )
        {
            e.printStackTrace();
            e.printStackTrace( System.out );
            return;
        }

        // spread the thread operations out so they don't all hit the DB at the
        // same time
        try
        {
            // GH issue 4 - throws exception if _target>1 because random.nextInt
            // argument must be >0
            // and the sleep() doesn't make sense for granularities < 1 ms
            // anyway
            if ( ( target > 0 ) && ( target <= 1.0 ) )
            {
                sleep( random.nextInt( 0, (int) ( 1.0 / target ) ) );
            }
        }
        catch ( InterruptedException e )
        {
            // do nothing.
        }

        try
        {
            long st = System.currentTimeMillis();

            switch ( benchmarkPhase )
            {
            case DATA_IMPORT:
                while ( ( ( operationCount == 0 ) || ( operationsDone < operationCount ) )
                        && !workload.isStopRequested() )
                {

                    if ( !workload.doTransaction( db, workloadState ) )
                    {
                        break;
                    }

                    operationsDone++;

                    // throttle the operations
                    if ( target > 0 )
                    {
                        // this is more accurate than other throttling
                        // approaches we have tried,
                        // like sleeping for (1/target throughput)-operation
                        // latency,
                        // because it smooths timing inaccuracies (from sleep()
                        // taking an int,
                        // current time in millis) over many operations
                        while ( System.currentTimeMillis() - st < ( (double) operationsDone ) / target )
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
                break;

            case TRANSACTIONS:
                while ( ( ( operationCount == 0 ) || ( operationsDone < operationCount ) )
                        && !workload.isStopRequested() )
                {

                    if ( !workload.doTransaction( db, workloadState ) )
                    {
                        break;
                    }

                    operationsDone++;

                    // throttle the operations
                    if ( target > 0 )
                    {
                        // this is more accurate than other throttling
                        // approaches we have tried,
                        // like sleeping for (1/target throughput)-operation
                        // latency,
                        // because it smooths timing inaccuracies (from sleep()
                        // taking an int,
                        // current time in millis) over many operations
                        while ( System.currentTimeMillis() - st < ( (double) operationsDone ) / target )
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
                break;
            }
        }
        catch ( Exception e )
        {
            // TODO this is bullshit, add proper error handling
            e.printStackTrace();
            e.printStackTrace( System.out );
            System.exit( 0 );
        }

        try
        {
            db.cleanup();
        }
        catch ( DBException e )
        {
            e.printStackTrace();
            e.printStackTrace( System.out );
            return;
        }
    }
}
