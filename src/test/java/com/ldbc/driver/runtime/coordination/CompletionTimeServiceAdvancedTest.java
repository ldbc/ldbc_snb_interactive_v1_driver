package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.testutils.TestUtils;
import com.ldbc.driver.testutils.ThreadPoolLoadGenerator;
import com.ldbc.driver.workloads.simple.SimpleWorkload;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.ldbc.driver.control.ConsoleAndFileDriverConfiguration.fromDefaults;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class CompletionTimeServiceAdvancedTest
{
    private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();
    private final TimeSource timeSource = new SystemTimeSource();
    private final CompletionTimeServiceAssistant completionTimeServiceAssistant = new CompletionTimeServiceAssistant();

    @Ignore
    @Test
    public void stressTestThreadedQueuedCompletionTimeService()
            throws InterruptedException, ExecutionException, WorkloadException, CompletionTimeException,
            DriverConfigurationException, IOException
    {
        ThreadPoolLoadGenerator threadPoolLoadGenerator = TestUtils.newThreadPoolLoadGenerator( 128, 0 );
        threadPoolLoadGenerator.start();
        try
        {
            int testRepetitions = 10;

            ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
            long totalTestDurationAsMilli;

            for ( int workerThreads = 1; workerThreads < 33; workerThreads = workerThreads * 2 )
            {
                totalTestDurationAsMilli = 0;
                for ( int i = 0; i < testRepetitions; i++ )
                {
                    CompletionTimeService cts = completionTimeServiceAssistant.newThreadedQueuedCompletionTimeService(
                            timeSource,
                            errorReporter );
                    try
                    {
                        totalTestDurationAsMilli += parallelCompletionTimeServiceTest(
                                cts,
                                errorReporter,
                                workerThreads );
                    }
                    finally
                    {
                        cts.shutdown();
                    }
                }
                System.out.printf( "\t%s=%s\n",
                        ThreadedQueuedCompletionTimeService.class.getSimpleName(),
                        TEMPORAL_UTIL.milliDurationToString( totalTestDurationAsMilli / testRepetitions ) );
            }
        }
        finally
        {
            threadPoolLoadGenerator.shutdown( TimeUnit.SECONDS.toMillis( 10 ) );
        }
    }

    @Test
    public void completionTimeServicesShouldBehaveDeterministically()
            throws InterruptedException, ExecutionException, WorkloadException, CompletionTimeException,
            DriverConfigurationException, IOException
    {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        int testRepetitions = 5;
        long totalTestDurationForSynchronousCts;
        long totalTestDurationForThreadedCts;

        for ( int workerThreads = 1; workerThreads < 33; workerThreads = workerThreads * 2 )
        {

            totalTestDurationForSynchronousCts = 0;
            for ( int i = 0; i < testRepetitions; i++ )
            {
                CompletionTimeService cts = completionTimeServiceAssistant.newSynchronizedCompletionTimeService();
                totalTestDurationForSynchronousCts += parallelCompletionTimeServiceTest(
                        cts,
                        errorReporter,
                        workerThreads );
                cts.shutdown();
            }
            System.out.printf( "Threads:%-2s\t%s=%s",
                    workerThreads,
                    SynchronizedCompletionTimeService.class.getSimpleName(),
                    TEMPORAL_UTIL.milliDurationToString( totalTestDurationForSynchronousCts / testRepetitions ) );

            totalTestDurationForThreadedCts = 0;
            for ( int i = 0; i < testRepetitions; i++ )
            {
                CompletionTimeService cts = completionTimeServiceAssistant.newThreadedQueuedCompletionTimeService(
                        timeSource,
                        errorReporter );
                totalTestDurationForThreadedCts += parallelCompletionTimeServiceTest(
                        cts,
                        errorReporter,
                        workerThreads );
                cts.shutdown();
            }
            System.out.printf( "\t%s=%s\n",
                    ThreadedQueuedCompletionTimeService.class.getSimpleName(),
                    TEMPORAL_UTIL.milliDurationToString( totalTestDurationForThreadedCts / testRepetitions ) );
        }
    }

    private long parallelCompletionTimeServiceTest(
            CompletionTimeService completionTimeService,
            ConcurrentErrorReporter errorReporter,
            int threadCount )
            throws WorkloadException, InterruptedException, ExecutionException, CompletionTimeException,
            DriverConfigurationException, IOException
    {
        // initialize executor
        ThreadFactory threadFactory = new ThreadFactory()
        {
            private final long factoryTimeStampId = System.currentTimeMillis();
            int count = 0;

            @Override
            public Thread newThread( Runnable runnable )
            {
                return new Thread(
                        runnable,
                        CompletionTimeServiceAdvancedTest.class.getSimpleName() +
                        ".parallelCompletionTimeServiceTest-id(" + factoryTimeStampId + ")" + "-thread(" + count++ +
                        ")" );
            }
        };
        ExecutorService executorService = Executors.newFixedThreadPool( threadCount, threadFactory );
        CompletionService<Integer> completionService = new ExecutorCompletionService<>( executorService );

        // initialize workload
        int operationCountCheckPoint1 = 100;
        int operationCountCheckPoint2 = 900;
        long operationCountAdditionalOperations = 100000;
        long operationCount = operationCountCheckPoint1 +
                              operationCountCheckPoint2 +
                              operationCountAdditionalOperations;

        CompletionTimeWriter completionTimeWriter = completionTimeService.newCompletionTimeWriter();

        ConsoleAndFileDriverConfiguration configuration = fromDefaults( null, null, operationCount );

        // TODO consider using DummyWorkload instead
        Workload workload = new SimpleWorkload();
        workload.init( configuration );
        GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42L ) );

        Iterator<Operation> operations = gf.limit(
                WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators(
                        gf,
                        workload.streams( gf, true ) ),
                configuration.operationCount() );

        // measure duration of experiment
        long startTimeAsMilli = timeSource.nowAsMilli();

        // track number of operations that have completed, i.e., that have their Completed Time submitted
        int completedOperations = 0;

        /*
        CREATE 1st CHECK POINT
         */
        Operation ctCheckpointOperation1 = operations.next();
        long ctCheckpointOperation1ScheduledStartTimeAsMilli = ctCheckpointOperation1.timeStamp();
        completionTimeWriter.submitInitiatedTime( ctCheckpointOperation1ScheduledStartTimeAsMilli );
        completionTimeWriter.submitCompletedTime( ctCheckpointOperation1ScheduledStartTimeAsMilli );
        completedOperations++;

        // This is only used for ensuring that time stamps are in fact monotonically increasing
        long lastScheduledStartTimeAsMilli = ctCheckpointOperation1ScheduledStartTimeAsMilli;

        long previousLastScheduledStartTimeAsMilli = -1;
        for ( int i = completedOperations; i < operationCountCheckPoint1; i++ )
        {
            Operation operation = operations.next();
            assertThat( operation.timeStamp() >= lastScheduledStartTimeAsMilli, is( true ) );
            previousLastScheduledStartTimeAsMilli = lastScheduledStartTimeAsMilli;
            lastScheduledStartTimeAsMilli = operation.timeStamp();
            completionTimeWriter.submitInitiatedTime( operation.timeStamp() );
            completionService.submit( new CtAccessingCallable( operation, completionTimeWriter, errorReporter ) );
        }

        // Wait for tasks to finish submitting Completed Times, up to 1st check point
        while ( completedOperations < operationCountCheckPoint1 )
        {
            completionService.take();
            completedOperations++;
        }

        assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

        /*
        TEST 1st CHECK POINT
         */
        assertThat( completionTimeService.completionTimeAsMilliFuture().get(),
                equalTo( previousLastScheduledStartTimeAsMilli ) );

        /*
        CREATE 2nd CHECK POINT
         */
        Operation ctCheckpointOperation2 = operations.next();
        long ctCheckpointOperation2ScheduledStartTimeAsMilli = ctCheckpointOperation2.timeStamp();
        completionTimeWriter.submitInitiatedTime( ctCheckpointOperation2ScheduledStartTimeAsMilli );
        completionTimeWriter.submitCompletedTime( ctCheckpointOperation2ScheduledStartTimeAsMilli );
        completedOperations++;

        for ( int i = completedOperations; i < operationCountCheckPoint2; i++ )
        {
            Operation operation = operations.next();
            assertThat( operation.timeStamp() >= lastScheduledStartTimeAsMilli, is( true ) );
            previousLastScheduledStartTimeAsMilli = lastScheduledStartTimeAsMilli;
            lastScheduledStartTimeAsMilli = operation.timeStamp();
            completionTimeWriter.submitInitiatedTime( operation.timeStamp() );
            completionService.submit( new CtAccessingCallable( operation, completionTimeWriter, errorReporter ) );
        }

        // Wait for tasks to finish submitting Completed Times, up to 2nd check point
        while ( completedOperations < operationCountCheckPoint2 )
        {
            completionService.take();
            completedOperations++;
        }

        assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

        /*
        TEST 2nd CHECK POINT
         */
        assertThat( completionTimeService.completionTimeAsMilliFuture().get(),
                equalTo( previousLastScheduledStartTimeAsMilli ) );

        while ( operations.hasNext() )
        {
            Operation operation = operations.next();
            assertThat( operation.timeStamp() >= lastScheduledStartTimeAsMilli, is( true ) );
            lastScheduledStartTimeAsMilli = operation.timeStamp();
            completionTimeWriter.submitInitiatedTime( operation.timeStamp() );
            completionService.submit( new CtAccessingCallable( operation, completionTimeWriter, errorReporter ) );
        }

        // Wait for tasks to finish submitting Completed Times, up to final check point (end of workload)
        while ( completedOperations < operationCount )
        {
            completionService.take();
            completedOperations++;
        }

        assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

        // Submit one more initiated time to allow completion time to advance to the last submitted
        // completed time
        long slightlyAfterLastScheduledStartTimeAsMilli = lastScheduledStartTimeAsMilli + 1;
        completionTimeWriter.submitInitiatedTime( slightlyAfterLastScheduledStartTimeAsMilli );

        /*
        TEST 3rd CHECK POINT
         */
        Future<Long> future3WaitingForLastOperation = completionTimeService.completionTimeAsMilliFuture();
        assertThat( future3WaitingForLastOperation.get(), equalTo( lastScheduledStartTimeAsMilli ) );

        long testDurationAsMilli = timeSource.nowAsMilli() - startTimeAsMilli;

        executorService.shutdown();
        boolean allTasksCompletedInTime = executorService.awaitTermination( 10, TimeUnit.SECONDS );
        assertThat( allTasksCompletedInTime, is( true ) );
        assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );
        workload.close();
        return testDurationAsMilli;
    }

    class CtAccessingCallable implements Callable<Integer>
    {
        private final Operation operation;
        private final CompletionTimeWriter completionTimeWriter;
        private final ConcurrentErrorReporter errorReporter;

        private CtAccessingCallable( Operation operation,
                CompletionTimeWriter completionTimeWriter,
                ConcurrentErrorReporter errorReporter )
        {
            this.operation = operation;
            this.completionTimeWriter = completionTimeWriter;
            this.errorReporter = errorReporter;
        }

        public Integer call() throws Exception
        {
            try
            {
                // operation completes
                completionTimeWriter.submitCompletedTime( operation.timeStamp() );
                return 1;
            }
            catch ( Exception e )
            {
                errorReporter.reportError( this, "Error in call()" );
                return -1;
            }
        }
    }

}
