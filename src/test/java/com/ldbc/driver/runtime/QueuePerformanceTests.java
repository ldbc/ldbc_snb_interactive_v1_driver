package com.ldbc.driver.runtime;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.control.Log4jLoggingServiceFactory;
import com.ldbc.driver.control.LoggingServiceFactory;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.testutils.TestUtils;
import com.ldbc.driver.util.Tuple3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveDb;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

@Ignore
public class QueuePerformanceTests
{
    final Operation TERMINATE_OPERATION = new Operation<Object>()
    {
        @Override
        public int type()
        {
            return -1;
        }

        @Override
        public Object marshalResult( String serializedOperationResult ) throws SerializingMarshallingException
        {
            return null;
        }

        @Override
        public String serializeResult( Object operationResultInstance ) throws SerializingMarshallingException
        {
            return null;
        }
    };

    final Integer TERMINATE = -1;
    final TimeSource timeSource = new SystemTimeSource();

    @Test
    public void operationQueuePerformanceTest()
            throws WorkloadException, InterruptedException, IOException, DriverConfigurationException
    {
        // Given
        Map<String,String> paramsMap = LdbcSnbInteractiveWorkloadConfiguration.defaultReadOnlyConfigSF1();
        paramsMap.put( LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                TestUtils.getResource( "/" ).getAbsolutePath() );
        // LDBC Interactive Workload-specific parameters
        // Driver-specific parameters
        String name = null;
        String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 10000000;
        int threadCount = 1;
        int statusDisplayInterval = 0;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = null;
        double timeCompressionRatio = 1.0;
        Set<String> peerIds = new HashSet<>();
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean calculateWorkloadStatistics = false;
        long spinnerSleepDuration = 0l;
        boolean printHelp = false;
        boolean ignoreScheduledStartTimes = false;
        long warmupCount = 0;
        long skipCount = 0;

        DriverConfiguration config = new ConsoleAndFileDriverConfiguration(
                paramsMap,
                name,
                dbClassName,
                workloadClassName,
                operationCount,
                threadCount,
                statusDisplayInterval,
                timeUnit,
                resultDirPath,
                timeCompressionRatio,
                peerIds,
                validationParams,
                dbValidationFilePath,
                calculateWorkloadStatistics,
                spinnerSleepDuration,
                printHelp,
                ignoreScheduledStartTimes,
                warmupCount,
                skipCount
        );

        GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42L ) );
        boolean returnStreamsWithDbConnector = false;
        LoggingServiceFactory loggingServiceFactory = new Log4jLoggingServiceFactory( false );
        Tuple3<WorkloadStreams,Workload,Long> workloadStreamsAndWorkload =
                WorkloadStreams.createNewWorkloadWithOffsetAndLimitedWorkloadStreams(
                        config,
                        gf,
                        returnStreamsWithDbConnector,
                        0,
                        config.operationCount(),
                        loggingServiceFactory
                );
        WorkloadStreams workloadStreams = workloadStreamsAndWorkload._1();
        Workload workload = workloadStreamsAndWorkload._2();

        Iterator<Operation> operations =
                WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators( gf, workloadStreams );

        System.out.println( "Benchmarking..." );

//        long duration = doOperationQueuePerformanceTest(operations, DefaultQueues.<Operation>newBlockingBounded
// (1000));
        long duration =
                doOperationQueuePerformanceTest( operations, DefaultQueues.<Operation>newBlockingBounded( 10000 ) );
        long opsPerSecond = Math.round(
                ((double) config.operationCount() / TimeUnit.MILLISECONDS.toNanos( duration )) * 1000000000 );
        System.out.println(
                format( "%s operations in %s: %s op/sec", config.operationCount(), duration, opsPerSecond ) );
        workload.close();
    }

    private long doOperationQueuePerformanceTest( final Iterator<Operation> operations, final Queue<Operation> queue )
            throws InterruptedException
    {
        final QueueEventSubmitter<Operation> queueEventSubmitter = QueueEventSubmitter.queueEventSubmitterFor( queue );
        final QueueEventFetcher<Operation> queueEventFetcher = QueueEventFetcher.queueEventFetcherFor( queue );
        Thread writeThread = new Thread()
        {
            @Override
            public void run()
            {
                while ( operations.hasNext() )
                {
                    try
                    {
                        queueEventSubmitter.submitEventToQueue( operations.next() );
                    }
                    catch ( InterruptedException e )
                    {
                        e.printStackTrace();
                    }
                }
                try
                {
                    queueEventSubmitter.submitEventToQueue( TERMINATE_OPERATION );
                }
                catch ( InterruptedException e )
                {
                    e.printStackTrace();
                }
                System.out.println( "Producer finished" );
            }
        };

        Thread readThread = new Thread()
        {
            @Override
            public void run()
            {
                Operation operation = null;
                try
                {
                    operation = queueEventFetcher.fetchNextEvent();
                }
                catch ( InterruptedException e )
                {
                    e.printStackTrace();
                }
                while ( TERMINATE_OPERATION != operation )
                {
                    try
                    {
                        operation = queueEventFetcher.fetchNextEvent();
                    }
                    catch ( InterruptedException e )
                    {
                        e.printStackTrace();
                    }
                }
                System.out.println( "Consumer finished" );
            }
        };

        long startTime = timeSource.nowAsMilli();
        readThread.start();
        writeThread.start();
        writeThread.join();
        readThread.join();

        return timeSource.nowAsMilli() - startTime;
    }


    @Ignore
    @Test
    public void comparePerformanceOfQueueImplementationsDuringConcurrentAccess() throws InterruptedException
    {
        int queueItemCount = 1000000;
        int experimentCount = 5;

        long totalDurationConcurrentLinkedQueueNonBlocking = 0l;

        long totalDurationLinkedBlockingQueueNonBlocking = 0l;
        long totalDurationLinkedBlockingQueueBlocking = 0l;

        long totalDurationArrayBlockingQueueNonBlocking = 0l;
        long totalDurationArrayBlockingQueueBlocking = 0l;

        long totalDurationLinkedTransferQueueAddPollNonBlocking = 0l;
        long totalDurationLinkedTransferQueuePutTakeBlocking = 0l;

        long totalDurationSynchronousQueueBlocking = 0l;

        for ( int i = 0; i < experimentCount; i++ )
        {
            totalDurationConcurrentLinkedQueueNonBlocking =
                    totalDurationConcurrentLinkedQueueNonBlocking +
                    nonBlockingQueuePerformanceTest( queueItemCount, new ConcurrentLinkedQueue<Integer>() );

            totalDurationLinkedBlockingQueueNonBlocking =
                    totalDurationLinkedBlockingQueueNonBlocking +
                    nonBlockingQueuePerformanceTest( queueItemCount, new LinkedBlockingQueue<Integer>() );
            totalDurationLinkedBlockingQueueBlocking =
                    totalDurationLinkedBlockingQueueBlocking +
                    blockingQueuePerformanceTest( queueItemCount, new LinkedBlockingQueue<Integer>() );

            totalDurationArrayBlockingQueueNonBlocking =
                    totalDurationArrayBlockingQueueNonBlocking + nonBlockingQueuePerformanceTest( queueItemCount,
                            new ArrayBlockingQueue<Integer>( queueItemCount ) );
            totalDurationArrayBlockingQueueBlocking =
                    totalDurationArrayBlockingQueueBlocking +
                    blockingQueuePerformanceTest( queueItemCount, new ArrayBlockingQueue<Integer>( queueItemCount ) );

            totalDurationLinkedTransferQueueAddPollNonBlocking =
                    totalDurationLinkedTransferQueueAddPollNonBlocking +
                    nonBlockingQueuePerformanceTest( queueItemCount, new LinkedTransferQueue<Integer>() );
            totalDurationLinkedTransferQueuePutTakeBlocking =
                    totalDurationLinkedTransferQueuePutTakeBlocking +
                    blockingQueuePerformanceTest( queueItemCount, new LinkedTransferQueue<Integer>() );

            totalDurationSynchronousQueueBlocking =
                    totalDurationSynchronousQueueBlocking +
                    blockingQueuePerformanceTest( queueItemCount, new SynchronousQueue<Integer>() );
        }

        long concurrentLinkedQueueNonBlockingItemsPerMs =
                (queueItemCount * experimentCount) / totalDurationConcurrentLinkedQueueNonBlocking;

        long linkedBlockingQueueNonBlockingItemsPerMs =
                (queueItemCount * experimentCount) / totalDurationLinkedBlockingQueueNonBlocking;
        long linkedBlockingQueueBlockingItemsPerMs =
                (queueItemCount * experimentCount) / totalDurationLinkedBlockingQueueBlocking;

        long arrayBlockingQueueNonBlockingItemsPerMs =
                (queueItemCount * experimentCount) / totalDurationArrayBlockingQueueNonBlocking;
        long arrayBlockingQueueBlockingItemsPerMs =
                (queueItemCount * experimentCount) / totalDurationArrayBlockingQueueBlocking;

        long linkedTransferQueueNonBlockingItemsPerMs =
                (queueItemCount * experimentCount) / totalDurationLinkedTransferQueueAddPollNonBlocking;
        long linkedTransferQueueBlockingItemsPerMs =
                (queueItemCount * experimentCount) / totalDurationLinkedTransferQueuePutTakeBlocking;

        long synchronousQueueBlockingItemsPerMs =
                (queueItemCount * experimentCount) / totalDurationSynchronousQueueBlocking;


        System.out.println(
                "ConcurrentLinkedQueue(non-blocking) = \t" + concurrentLinkedQueueNonBlockingItemsPerMs + " item/ms" );

        System.out.println(
                "LinkedBlockingQueue(non-blocking) = \t" + linkedBlockingQueueNonBlockingItemsPerMs + " item/ms" );
        System.out
                .println( "LinkedBlockingQueue(blocking) = \t\t" + linkedBlockingQueueBlockingItemsPerMs + " item/ms" );

        System.out.println(
                "ArrayBlockingQueue(non-blocking) = \t\t" + arrayBlockingQueueNonBlockingItemsPerMs + " item/ms" );
        System.out
                .println( "ArrayBlockingQueue(blocking) = \t\t\t" + arrayBlockingQueueBlockingItemsPerMs + " item/ms" );

        System.out.println(
                "LinkedTransferQueue(non-blocking) = \t" + linkedTransferQueueNonBlockingItemsPerMs + " item/ms" );
        System.out
                .println( "LinkedTransferQueue(blocking) = \t\t" + linkedTransferQueueBlockingItemsPerMs + " item/ms" );

        System.out.println( "SynchronousQueue(blocking) = \t\t\t" + synchronousQueueBlockingItemsPerMs + " item/ms" );
    }

    public long nonBlockingQueuePerformanceTest( final int queueItemCount, final Queue<Integer> queue )
            throws InterruptedException
    {
        Thread writeThread = new Thread()
        {
            @Override
            public void run()
            {
                for ( int i = 0; i < queueItemCount; i++ )
                {
                    queue.add( i );
                }
                queue.add( TERMINATE );
            }
        };

        Thread readThread = new Thread()
        {
            @Override
            public void run()
            {
                Integer val = 0;
                while ( TERMINATE.equals( val ) == false )
                {
                    val = queue.poll();
                }
            }
        };

        long startTimeAsMilli = timeSource.nowAsMilli();
        readThread.start();
        writeThread.start();
        writeThread.join();
        readThread.join();
        assertThat( queue.poll(), is( nullValue() ) );
        assertThat( queue.size(), is( 0 ) );
        return timeSource.nowAsMilli() - startTimeAsMilli;
    }

    public long blockingQueuePerformanceTest( final int queueItemCount, final BlockingQueue<Integer> queue )
            throws InterruptedException
    {
        Thread writeThread = new Thread()
        {
            @Override
            public void run()
            {
                try
                {
                    for ( int i = 0; i < queueItemCount; i++ )
                    {
                        queue.put( i );
                    }
                    queue.put( TERMINATE );
                }
                catch ( InterruptedException e )
                {
                    e.printStackTrace();
                }
            }
        };

        Thread readThread = new Thread()
        {
            @Override
            public void run()
            {
                try
                {
                    Integer val = 0;
                    while ( TERMINATE.equals( val ) == false )
                    {
                        val = queue.take();
                    }
                }
                catch ( InterruptedException e )
                {
                    e.printStackTrace();
                }
            }
        };

        long startTimeAsMilli = timeSource.nowAsMilli();
        readThread.start();
        writeThread.start();
        writeThread.join();
        readThread.join();
        assertThat( queue.poll(), is( nullValue() ) );
        assertThat( queue.size(), is( 0 ) );
        return timeSource.nowAsMilli() - startTimeAsMilli;
    }
}
