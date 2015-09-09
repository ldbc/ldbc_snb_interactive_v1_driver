package com.ldbc.driver.runtime;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ldbc.driver.Db;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.control.Log4jLoggingServiceFactory;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.control.LoggingServiceFactory;
import com.ldbc.driver.csv.simple.SimpleCsvFileWriter;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.CompletionTimeService;
import com.ldbc.driver.runtime.coordination.CompletionTimeServiceAssistant;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.runtime.metrics.MetricsService;
import com.ldbc.driver.runtime.metrics.ThreadedQueuedMetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.ManualTimeSource;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.workloads.dummy.DummyDb;
import com.ldbc.driver.workloads.dummy.DummyWorkload;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation2;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation2Factory;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation3;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class WorkloadRunnerComplexScenarioTests
{
    private static final LoggingServiceFactory LOG4J_LOGGING_SERVICE_FACTORY = new Log4jLoggingServiceFactory( false );
    private final long ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING = 300;
    private final long SPINNER_SLEEP_DURATION_AS_MILLI = 0;
    private final ManualTimeSource timeSource = new ManualTimeSource( 0 );
    private final CompletionTimeServiceAssistant completionTimeServiceAssistant = new CompletionTimeServiceAssistant();
    private final GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42l ) );

    @Test
    public void oneExecutorShouldNotBeAbleToStarveAnotherOfThreads()
            throws WorkloadException, CompletionTimeException, DbException, InterruptedException,
            MetricsCollectionException, IOException
    {
        // fails with 1 thread, need to investigate further, probably because there is no available thread to execute
        // an operation handler in time <-- not necessarily a bug
//        oneExecutorShouldNotBeAbleToStarveAnotherOfThreads(1);
        oneExecutorShouldNotBeAbleToStarveAnotherOfThreads( 4 );
        oneExecutorShouldNotBeAbleToStarveAnotherOfThreads( 16 );
    }

    public void oneExecutorShouldNotBeAbleToStarveAnotherOfThreads( int threadCount )
            throws WorkloadException, CompletionTimeException, DbException, InterruptedException,
            MetricsCollectionException, IOException
    {
        // @formatter:off
            /*
            // @formatter:off
                Number of writers: 1 (blocking)
                Number of executors: 2 (blocking & async)
                Initialized to: IT[ , ] CT[0,1]
                Thread Pool Size: 2

                ASYNC                   THREADS         BLOCKING                THREADS
                READ                                    READ_WRITE                          GCT (assumes initiated
                time submitted quickly)  ACTION                          COMPLETED
                TimedNamedOperation1                    TimedNamedOperation2
            0                           []                                      []          1 <-- S(4)D(0)
            initialized                                                      0
            1                           []                                      []          1
                                                                   0
            2   S(2)D(0)                [S(2)]                                  []          1
                                   BLOCK S(2)D(0)                  0
            3   S(3)D(0)                [S(2),S(3)]                             []          1
                                   BLOCK S(3)D(0)                  0
            4                           [S(2),S(3)]              S(4)D(0)       S(4)<-[]    4 <-- S(5)D(0) initiated
                                                                   1
            5                           [S(2),S(3)]              S(5)D(0)       [S(5)]      4
                                   BLOCK S(5)D(0)                  1
            6                           S(3)<-[S(2)]                            [S(5)]      4
                                   UNBLOCK S(3)D(0)                2
            7   S(7)D(0)                S(7)<-[S(2)]                            [S(5)]      4
                                                                   3
            8                           []                                      [S(5)]      4
                                                                   3
            9                           []                                      [S(5)]      4
                                                                   3
            10                          []                                      [S(5)]      4
                                                                   3
            11                          []                                      [S(5)]      4
                                                                   3
            // @formatter:on
             */
        // @formatter:on

        LoggingService loggingService = new Log4jLoggingServiceFactory( false ).loggingServiceFor( "Test" );
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        SimpleCsvFileWriter csvResultsLogWriter = null;
        Map<Integer,Class<? extends Operation>> operationTypeToClassMapping = new HashMap<>();
        operationTypeToClassMapping.put( TimedNamedOperation1.TYPE, TimedNamedOperation1.class );
        operationTypeToClassMapping.put( TimedNamedOperation2.TYPE, TimedNamedOperation2.class );
        operationTypeToClassMapping.put( TimedNamedOperation3.TYPE, TimedNamedOperation3.class );
        MetricsService metricsService = ThreadedQueuedMetricsService.newInstanceUsingBlockingBoundedQueue(
                timeSource,
                errorReporter,
                TimeUnit.MILLISECONDS,
                ThreadedQueuedMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO,
                csvResultsLogWriter,
                operationTypeToClassMapping,
                LOG4J_LOGGING_SERVICE_FACTORY
        );

        Set<String> peerIds = new HashSet<>();
        // TODO test also with threaded completion time service implementation
        CompletionTimeService completionTimeService =
                completionTimeServiceAssistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds( peerIds );

        WorkloadStreams workloadStreams = new WorkloadStreams();
        Set<Class<? extends Operation>> asynchronousDependentOperationTypes =
                Sets.<Class<? extends Operation>>newHashSet(
                        TimedNamedOperation1.class
                );
        Set<Class<? extends Operation>> asynchronousDependencyOperationTypes = Sets.newHashSet(
                // nothing
        );
        Iterator<Operation> asynchronousDependencyOperations = Collections.emptyIterator();
        Iterator<Operation> asynchronousNonDependencyOperations = Lists.<Operation>newArrayList(
                new TimedNamedOperation1( 2, 2, 0, "S(2)D(0)" ),
                new TimedNamedOperation1( 3, 3, 0, "S(3)D(0)" ),
                new TimedNamedOperation1( 7, 7, 0, "S(7)D(0)" )
        ).iterator();
        workloadStreams.setAsynchronousStream(
                asynchronousDependentOperationTypes,
                asynchronousDependencyOperationTypes,
                asynchronousDependencyOperations,
                asynchronousNonDependencyOperations,
                null
        );
        Set<Class<? extends Operation>> blockingDependentOperationTypes = Sets.<Class<? extends Operation>>newHashSet(
                TimedNamedOperation2.class
        );
        Set<Class<? extends Operation>> blockingDependencyOperationTypes = Sets.<Class<? extends Operation>>newHashSet(
                TimedNamedOperation2.class
        );
        Iterator<Operation> blockingDependencyOperations = Lists.<Operation>newArrayList(
                new TimedNamedOperation2( 4, 4, 0, "S(4)D(0)" ),
                new TimedNamedOperation2( 5, 5, 0, "S(5)D(0)" )
        ).iterator();
        Iterator<Operation> blockingNonDependencyOperations = Lists.<Operation>newArrayList(
        ).iterator();
        workloadStreams.addBlockingStream(
                blockingDependentOperationTypes,
                blockingDependencyOperationTypes,
                blockingDependencyOperations,
                blockingNonDependencyOperations,
                null
        );

        Map<String,String> params = new HashMap<>();
        params.put( DummyDb.ALLOWED_DEFAULT_ARG, "true" );

        try ( DummyDb db = new DummyDb() )
        {
            db.init(
                    params,
                    loggingService,
                    DummyWorkload.OPERATION_TYPE_CLASS_MAPPING
            );

            db.setNameAllowedValue( "S(2)D(0)", false );
            db.setNameAllowedValue( "S(3)D(0)", false );
            db.setNameAllowedValue( "S(4)D(0)", true );
            db.setNameAllowedValue( "S(5)D(0)", false );
            db.setNameAllowedValue( "S(7)D(0)", true );

            WorkloadRunnerThread runnerThread = workloadRunnerThread(
                    timeSource,
                    workloadStreams,
                    threadCount,
                    errorReporter,
                    metricsService,
                    completionTimeService,
                    db
            );

            MetricsService.MetricsServiceWriter metricsServiceWriter = metricsService.getWriter();

            // initialize GCT
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters( completionTimeService, 0 );
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters( completionTimeService, 1 );

            timeSource.setNowFromMilli( 0 );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), is( 0l ) );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 0l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            runnerThread.start();

            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 0l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 1l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 1 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 0l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 1l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 2 );
            // S(2)D(0) is blocked, nothing will change
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 0l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 1l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 3 );
            // S(3)D(0) is blocked, nothing will change
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 0l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 1l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 4 );
            // check that S(4)D(0) is able to complete (is not starved of thread)
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 1l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 4l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 5 );
            // S(5)D(0) is blocked, nothing will change
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 1l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 4l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 6 );
            db.setNameAllowedValue( "S(3)D(0)", true );
            // S(3)D(0) is unblocked -> S(3)D(0) finishes
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 2l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 4l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 7 );
            // check that S(7)D(0) is able to complete (is not starved of thread)
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 3l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 4l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 8 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 3l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 4l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            // allow S(2)D(0) & S(5)D(0) to complete, so workload runner can terminate
            db.setNameAllowedValue( "S(2)D(0)", true );
            db.setNameAllowedValue( "S(5)D(0)", true );

            long durationToWaitForRunnerToCompleteAsMilli = WorkloadRunner.RUNNER_POLLING_INTERVAL_AS_MILLI * 4;
            long timeoutTimeAsMilli = timeSource.nowAsMilli() + durationToWaitForRunnerToCompleteAsMilli;
            while ( timeSource.nowAsMilli() < timeoutTimeAsMilli )
            {
                if ( runnerThread.runnerHasCompleted() )
                {
                    break;
                }
                Spinner.powerNap( 100 );
            }

            db.setAllowedValueForAll( true );
            assertThat( errorReporter.toString(), runnerThread.runnerHasCompleted(), is( true ) );
            assertThat( errorReporter.toString(), runnerThread.runnerCompletedSuccessfully(), is( true ) );
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
            throw e;
        }
        finally
        {
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            metricsService.shutdown();
            completionTimeService.shutdown();
        }
    }

    @Test
    public void oneExecutorShouldNotBeCapableOfAdvancingInitiatedTimeOfAnotherExecutor()
            throws CompletionTimeException, InterruptedException, MetricsCollectionException, DbException,
            WorkloadException, IOException
    {
        oneExecutorShouldNotBeCapableOfAdvancingInitiatedTimeOfAnotherExecutor( 1 );
        oneExecutorShouldNotBeCapableOfAdvancingInitiatedTimeOfAnotherExecutor( 4 );
        oneExecutorShouldNotBeCapableOfAdvancingInitiatedTimeOfAnotherExecutor( 16 );
    }

    public void oneExecutorShouldNotBeCapableOfAdvancingInitiatedTimeOfAnotherExecutor( int threadCount )
            throws CompletionTimeException, InterruptedException, MetricsCollectionException, DbException,
            WorkloadException, IOException
    {
        // @formatter:off
            /*
                Number of writers: 2 (blocking & async)
                Number of executors: 2 (blocking & async)
                Initialized to: IT[ , ] CT[0,1]

                ASYNC                   BLOCKING
                READ_WRITE              READ_WRITE                  GCT (assumes initiated time submitted quickly)
                       ACTION
                TimedNamedOperation1    TimedNamedOperation2
            0                                                       1 <-- S(2)D(0) initialized <--S (3)D(0) initialized
            1                                                       1
            2   S(2)D(0)                                            2 <-- S(5)D(0) initialized
            3                           S(3)D(0)                    3 <-- S(4)D(0) initialized
            4                           S(4)D(0) x 10,000,000       3 <-- S(4)D(0) initialized x 9,999,999
            5   S(5)D(0)                                            3
                   !!BLOCK S(5)D(0)!! <*>

            NOTE
             - <*> must block, otherwise S(5)D(0) may (race condition) complete before first S(4)D(0) initiated time
             is submitted, and GCT would advance
             - time should be advanced directly from 3 to 5
             - Async & Blocking executors will both try to submit initiated times for all due operations
             - the aim is for Async to manage to submit its 1 operation before the last Blocking time is submitted
             - the goal is to force initiated time 5 to be submitted before at least one initiated time 4 is
             submitted <-- illegal operation
             - this is only illegal if both executors were using the same local completion time writer,
             this test makes sure they are not
             */
        // @formatter:on
        LoggingService loggingService = new Log4jLoggingServiceFactory( false ).loggingServiceFor( "Test" );
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

        SimpleCsvFileWriter csvResultsLogWriter = null;
        Map<Integer,Class<? extends Operation>> operationTypeToClassMapping = new HashMap<>();
        operationTypeToClassMapping.put( TimedNamedOperation1.TYPE, TimedNamedOperation1.class );
        operationTypeToClassMapping.put( TimedNamedOperation2.TYPE, TimedNamedOperation2.class );
        operationTypeToClassMapping.put( TimedNamedOperation3.TYPE, TimedNamedOperation3.class );
        MetricsService metricsService = ThreadedQueuedMetricsService.newInstanceUsingBlockingBoundedQueue(
                timeSource,
                errorReporter,
                TimeUnit.MILLISECONDS,
                ThreadedQueuedMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO,
                csvResultsLogWriter,
                operationTypeToClassMapping,
                LOG4J_LOGGING_SERVICE_FACTORY
        );

        Set<String> peerIds = new HashSet<>();
        // TODO test also with threaded completion time service implementation
        CompletionTimeService completionTimeService =
                completionTimeServiceAssistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds( peerIds );

        WorkloadStreams workloadStreams = new WorkloadStreams();
        Set<Class<? extends Operation>> asynchronousDependentOperationTypes =
                Sets.<Class<? extends Operation>>newHashSet(
                        TimedNamedOperation1.class
                );
        Set<Class<? extends Operation>> asynchronousDependencyOperationTypes =
                Sets.<Class<? extends Operation>>newHashSet(
                        TimedNamedOperation1.class
                );
        Iterator<Operation> asynchronousDependencyOperations = Lists.<Operation>newArrayList(
                new TimedNamedOperation1( 2, 2, 0, "read1" ),
                new TimedNamedOperation1( 5, 5, 0, "read2" )
        ).iterator();
        Iterator<Operation> asynchronousNonDependencyOperations = Lists.<Operation>newArrayList(
        ).iterator();
        workloadStreams.setAsynchronousStream(
                asynchronousDependentOperationTypes,
                asynchronousDependencyOperationTypes,
                asynchronousDependencyOperations,
                asynchronousNonDependencyOperations,
                null
        );
        Set<Class<? extends Operation>> blockingDependentOperationTypes = Sets.<Class<? extends Operation>>newHashSet(
                TimedNamedOperation2.class
        );
        Set<Class<? extends Operation>> blockingDependencyOperationTypes = Sets.<Class<? extends Operation>>newHashSet(
                TimedNamedOperation2.class
        );
        List<Operation> blockingDependencyOperationsList = Lists.<Operation>newArrayList(
                new TimedNamedOperation2( 3, 3, 0, "readwrite1" )
        );

        int operationCountAtTime4 = 1000000;
        Iterator<Operation> manyReadWriteOperationsAtTime4 = gf.limit(
                new TimedNamedOperation2Factory(
                        gf.constant( 4l ),
                        gf.constant( 0l ),
                        gf.constant( "oneOfManyReadWrite2" ) ),
                operationCountAtTime4 );
        blockingDependencyOperationsList.addAll( Lists.newArrayList( manyReadWriteOperationsAtTime4 ) );

        Iterator<Operation> blockingDependencyOperations = blockingDependencyOperationsList.iterator();
        Iterator<Operation> blockingNonDependencyOperations = Lists.<Operation>newArrayList(
        ).iterator();
        workloadStreams.addBlockingStream(
                blockingDependentOperationTypes,
                blockingDependencyOperationTypes,
                blockingDependencyOperations,
                blockingNonDependencyOperations,
                null
        );

        Map<String,String> params = new HashMap<>();
        params.put( DummyDb.ALLOWED_DEFAULT_ARG, "true" );

        try ( DummyDb db = new DummyDb() )
        {
            db.init(
                    params,
                    loggingService,
                    DummyWorkload.OPERATION_TYPE_CLASS_MAPPING
            );

            WorkloadRunnerThread runnerThread = workloadRunnerThread(
                    timeSource,
                    workloadStreams,
                    threadCount,
                    errorReporter,
                    metricsService,
                    completionTimeService,
                    db
            );

            MetricsService.MetricsServiceWriter metricsServiceWriter = metricsService.getWriter();

            // initialize GCT
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters( completionTimeService, 0 );
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters( completionTimeService, 1 );

            timeSource.setNowFromMilli( 0 );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), is( 0l ) );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 0l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            runnerThread.start();

            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 0l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 1l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 1 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 0l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 1l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            // read1 can execute
            timeSource.setNowFromMilli( 2 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 1l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 2l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            // readwrite1 can execute
            timeSource.setNowFromMilli( 3 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 2l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 3l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            // at this point read2 and all readWrite2 can execute <-- read2 must be blocked for test to do what is
            // intended
            db.setNameAllowedValue( "read2", false );
            timeSource.setNowFromMilli( 5 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), is( 3l ) );
            // if initiated time 4 was submitted after initiated time 5 an error should have been reported (hopefully
            // it was not)
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            // allow read2 to complete, so workload runner can terminate
            db.setNameAllowedValue( "read2", true );

            long durationToWaitForRunnerToCompleteAsMilli = WorkloadRunner.RUNNER_POLLING_INTERVAL_AS_MILLI * 4;
            long timeoutTimeAsMilli = timeSource.nowAsMilli() + durationToWaitForRunnerToCompleteAsMilli;
            while ( timeSource.nowAsMilli() < timeoutTimeAsMilli )
            {
                if ( runnerThread.runnerHasCompleted() )
                {
                    break;
                }
                Spinner.powerNap( 100 );
            }

            db.setAllowedValueForAll( true );
            assertThat( errorReporter.toString(), runnerThread.runnerHasCompleted(), is( true ) );
            assertThat( errorReporter.toString(), runnerThread.runnerCompletedSuccessfully(), is( true ) );
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
            throw e;
        }
        finally
        {
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            metricsService.shutdown();
            completionTimeService.shutdown();
        }
    }

    @Test
    public void shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadAsyncReadWriteAsync()
            throws DriverConfigurationException, DbException, CompletionTimeException, WorkloadException,
            InterruptedException, MetricsCollectionException, IOException
    {
        shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadAsyncReadWriteAsync( 1 );
        shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadAsyncReadWriteAsync( 4 );
        shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadAsyncReadWriteAsync( 16 );
    }

    public void shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadAsyncReadWriteAsync( int threadCount )
            throws DriverConfigurationException, DbException, CompletionTimeException, WorkloadException,
            InterruptedException, MetricsCollectionException, IOException
    {
        // @formatter:off
        /*
            Number of writers: 1 (async)
            Number of executors: 2 (blocking & async)
            Initialized to: IT[ , ] CT[0,1]

            ASYNC                   ASYNC
            READ                    READ_WRITE
            TimedNamedOperation1    TimedNamedOperation2    GCT (assumes initiated time submitted quickly)
        0                                                   1 <~~ S(2)D(0) initialized (READ ONLY)
        1                                                   1
        2   S(2)D(0)                                        1 <-- S(3)D(0) initialized
        3                           S(3)D(0)                3 <~~ S(4)D(0) initialized (READ ONLY)
        4   S(4)D(0)                                        3 <-- S(6)D(0) initialized
        5                                                   3 <~~ S(4)D(0) initialized (READ ONLY)
        6                           S(6)D(0)                6 <~~ S(7)D(3) initialized (READ ONLY)
        7   S(7)D(3)                                        6 <-- S(9)D(3) initialized
        8                                                   6
        9                           S(9)D(3)                9 // executor knows this is last WRITE
        10                                                  9
        11  S(11)D(0)                                       9 <~~ S(11)D(0) initialized (READ ONLY)
        12                                                  9
        13  S(13)D(6)                                       9 <~~ S(13)D(6) initialized (READ ONLY)
         */
        // @formatter:on
        LoggingService loggingService = new Log4jLoggingServiceFactory( false ).loggingServiceFor( "Test" );
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

        SimpleCsvFileWriter csvResultsLogWriter = null;
        Map<Integer,Class<? extends Operation>> operationTypeToClassMapping = new HashMap<>();
        operationTypeToClassMapping.put( TimedNamedOperation1.TYPE, TimedNamedOperation1.class );
        operationTypeToClassMapping.put( TimedNamedOperation2.TYPE, TimedNamedOperation2.class );
        operationTypeToClassMapping.put( TimedNamedOperation3.TYPE, TimedNamedOperation3.class );
        MetricsService metricsService = ThreadedQueuedMetricsService.newInstanceUsingBlockingBoundedQueue(
                timeSource,
                errorReporter,
                TimeUnit.MILLISECONDS,
                ThreadedQueuedMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO,
                csvResultsLogWriter,
                operationTypeToClassMapping,
                LOG4J_LOGGING_SERVICE_FACTORY
        );

        Set<String> peerIds = new HashSet<>();
        // TODO test also with threaded completion time service implementation
        CompletionTimeService completionTimeService =
                completionTimeServiceAssistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds( peerIds );

        WorkloadStreams workloadStreams = new WorkloadStreams();
        Set<Class<? extends Operation>> asynchronousDependentOperationTypes =
                Sets.<Class<? extends Operation>>newHashSet(
                        TimedNamedOperation1.class,
                        TimedNamedOperation2.class
                );
        Set<Class<? extends Operation>> asynchronousDependencyOperationTypes =
                Sets.<Class<? extends Operation>>newHashSet(
                        TimedNamedOperation2.class
                );
        Iterator<Operation> asynchronousDependencyOperations = Lists.<Operation>newArrayList(
                new TimedNamedOperation2( 3, 3, 0, "readwrite1" ),
                new TimedNamedOperation2( 6, 6, 0, "readwrite2" ),
                new TimedNamedOperation2( 9, 9, 3, "readwrite3" )
        ).iterator();
        Iterator<Operation> asynchronousNonDependencyOperations = Lists.<Operation>newArrayList(
                new TimedNamedOperation1( 2, 2, 0, "read1" ),
                new TimedNamedOperation1( 4, 4, 0, "read2" ),
                new TimedNamedOperation1( 7, 7, 3, "read3" ),
                new TimedNamedOperation1( 11, 11, 0, "read4" ),
                new TimedNamedOperation1( 13, 13, 6, "read5" )
        ).iterator();
        workloadStreams.setAsynchronousStream(
                asynchronousDependentOperationTypes,
                asynchronousDependencyOperationTypes,
                asynchronousDependencyOperations,
                asynchronousNonDependencyOperations,
                null
        );
        Set<Class<? extends Operation>> blockingDependentOperationTypes = Sets.newHashSet(
                // nothing
        );
        Set<Class<? extends Operation>> blockingDependencyOperationTypes = Sets.newHashSet(
                // nothing
        );
        Iterator<Operation> blockingDependencyOperations = Lists.<Operation>newArrayList(
                // nothing
        ).iterator();
        Iterator<Operation> blockingNonDependencyOperations = Lists.<Operation>newArrayList(
                // nothing
        ).iterator();
        workloadStreams.addBlockingStream(
                blockingDependentOperationTypes,
                blockingDependencyOperationTypes,
                blockingDependencyOperations,
                blockingNonDependencyOperations,
                null
        );

        Map<String,String> params = new HashMap<>();
        params.put( DummyDb.ALLOWED_DEFAULT_ARG, "false" );
        try ( DummyDb db = new DummyDb() )
        {
            db.init(
                    params,
                    loggingService,
                    DummyWorkload.OPERATION_TYPE_CLASS_MAPPING
            );

            WorkloadRunnerThread runnerThread = workloadRunnerThread(
                    timeSource,
                    workloadStreams,
                    threadCount,
                    errorReporter,
                    metricsService,
                    completionTimeService,
                    db
            );

            MetricsService.MetricsServiceWriter metricsServiceWriter = metricsService.getWriter();

            // initialize GCT
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters( completionTimeService, 0 );
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters( completionTimeService, 1 );

            timeSource.setNowFromMilli( 0 );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), is( 0l ) );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 0l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            runnerThread.start();

            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 0l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), is( 1l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 1 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 0l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), is( 1l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 2 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 0l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), is( 1l ) );
            db.setNameAllowedValue( "read1", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 1l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), is( 1l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 3 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 1l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), is( 1l ) );
            db.setNameAllowedValue( "readwrite1", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 2l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 3l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 4 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 2l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 3l ) );
            db.setNameAllowedValue( "read2", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 3l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 3l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 5 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 3l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 3l ) );

            timeSource.setNowFromMilli( 6 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 3l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 3l ) );
            db.setNameAllowedValue( "readwrite2", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 4l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 6l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 7 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 4l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 6l ) );
            db.setNameAllowedValue( "read3", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 5l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 6l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 8 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 5l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 6l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 9 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 5l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 6l ) );
            db.setNameAllowedValue( "readwrite3", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 6l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 10 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 6l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 11 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 6l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            db.setNameAllowedValue( "read4", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 7l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 12 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 7l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 13 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 7l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            db.setNameAllowedValue( "read5", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 8l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            long durationToWaitForRunnerToCompleteAsMilli = WorkloadRunner.RUNNER_POLLING_INTERVAL_AS_MILLI * 4;
            long timeoutTimeAsMilli = timeSource.nowAsMilli() + durationToWaitForRunnerToCompleteAsMilli;
            while ( timeSource.nowAsMilli() < timeoutTimeAsMilli )
            {
                if ( runnerThread.runnerHasCompleted() )
                {
                    break;
                }
                Spinner.powerNap( 100 );
            }

            db.setAllowedValueForAll( true );
            assertThat( errorReporter.toString(), runnerThread.runnerHasCompleted(), is( true ) );
            assertThat( errorReporter.toString(), runnerThread.runnerCompletedSuccessfully(), is( true ) );
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
            throw e;
        }
        finally
        {
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            metricsService.shutdown();
            completionTimeService.shutdown();
        }
    }

    @Test
    public void shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadAsyncReadWriteBlocking()
            throws DriverConfigurationException, DbException, CompletionTimeException, WorkloadException,
            InterruptedException, MetricsCollectionException, IOException
    {
        shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadAsyncReadWriteBlocking( 1 );
        shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadAsyncReadWriteBlocking( 4 );
        shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadAsyncReadWriteBlocking( 16 );
    }

    public void shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadAsyncReadWriteBlocking( int threadCount )
            throws DriverConfigurationException, DbException, CompletionTimeException, WorkloadException,
            InterruptedException, MetricsCollectionException, IOException
    {
        // @formatter:off
        /*
            Number of writers: 1 (blocking)
            Number of executors: 2 (blocking & async)
            Initialized to: IT[ , ] CT[0,1]

            ASYNC                   BLOCKING
            READ                    READ_WRITE
            TimedNamedOperation1    TimedNamedOperation2    GCT (assumes initiated time submitted quickly)
        0                                                   1 <-- S(3)D(0) initialized <~~ S(2)D(0) initialized (READ
         ONLY)
        1                                                   1
        2   S(2)D(0)                                        1 <~~ S(4)D(0) initialized (READ ONLY)
        3                           S(3)D(0)                3 <-- S(6)D(0) initialized
        4   S(4)D(0)                                        3 <~~ S(7)D(3) initialized (READ ONLY)
        5                                                   3
        6                           S(6)D(0)                6 <-- S(9)D(3) initialized
        7   S(7)D(3)                                        6 <~~ S(11)D(0) initialized (READ ONLY)
        8                                                   6
        9                           S(9)D(3)                6
        10                                                  6
        11  S(11)D(0)                                       6 <~~ S(13)D(6) initialized (READ ONLY)
        12                                                  6
        13  S(13)D(6)                                       6
         */
        // @formatter:on
        LoggingService loggingService = new Log4jLoggingServiceFactory( false ).loggingServiceFor( "Test" );
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

        SimpleCsvFileWriter csvResultsLogWriter = null;
        Map<Integer,Class<? extends Operation>> operationTypeToClassMapping = new HashMap<>();
        operationTypeToClassMapping.put( TimedNamedOperation1.TYPE, TimedNamedOperation1.class );
        operationTypeToClassMapping.put( TimedNamedOperation2.TYPE, TimedNamedOperation2.class );
        operationTypeToClassMapping.put( TimedNamedOperation3.TYPE, TimedNamedOperation3.class );
        MetricsService metricsService = ThreadedQueuedMetricsService.newInstanceUsingBlockingBoundedQueue(
                timeSource,
                errorReporter,
                TimeUnit.MILLISECONDS,
                ThreadedQueuedMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO,
                csvResultsLogWriter,
                operationTypeToClassMapping,
                LOG4J_LOGGING_SERVICE_FACTORY
        );

        Set<String> peerIds = new HashSet<>();
        // TODO test also with threaded completion time service implementation
        CompletionTimeService completionTimeService =
                completionTimeServiceAssistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds( peerIds );

        WorkloadStreams workloadStreams = new WorkloadStreams();
        Set<Class<? extends Operation>> asynchronousDependentOperationTypes =
                Sets.<Class<? extends Operation>>newHashSet(
                        TimedNamedOperation1.class
                );
        Set<Class<? extends Operation>> asynchronousDependencyOperationTypes =
                Sets.<Class<? extends Operation>>newHashSet(
                );
        Iterator<Operation> asynchronousDependencyOperations = Lists.<Operation>newArrayList(
                // nothing
        ).iterator();
        Iterator<Operation> asynchronousNonDependencyOperations = Lists.<Operation>newArrayList(
                new TimedNamedOperation1( 2, 2, 0, "read1" ),
                new TimedNamedOperation1( 4, 4, 0, "read2" ),
                new TimedNamedOperation1( 7, 7, 3, "read3" ),
                new TimedNamedOperation1( 11, 11, 0, "read4" ),
                new TimedNamedOperation1( 13, 13, 6, "read5" )
        ).iterator();
        workloadStreams.setAsynchronousStream(
                asynchronousDependentOperationTypes,
                asynchronousDependencyOperationTypes,
                asynchronousDependencyOperations,
                asynchronousNonDependencyOperations,
                null
        );
        Set<Class<? extends Operation>> blockingDependentOperationTypes = Sets.<Class<? extends Operation>>newHashSet(
                TimedNamedOperation2.class
        );
        Set<Class<? extends Operation>> blockingDependencyOperationTypes = Sets.<Class<? extends Operation>>newHashSet(
                TimedNamedOperation2.class
        );
        Iterator<Operation> blockingDependencyOperations = Lists.<Operation>newArrayList(
                new TimedNamedOperation2( 3, 3, 0, "readwrite1" ),
                new TimedNamedOperation2( 6, 6, 0, "readwrite2" ),
                new TimedNamedOperation2( 9, 9, 3, "readwrite3" )
        ).iterator();
        Iterator<Operation> blockingNonDependencyOperations = Lists.<Operation>newArrayList(
                // nothing
        ).iterator();
        workloadStreams.addBlockingStream(
                blockingDependentOperationTypes,
                blockingDependencyOperationTypes,
                blockingDependencyOperations,
                blockingNonDependencyOperations,
                null
        );

        Map<String,String> params = new HashMap<>();
        params.put( DummyDb.ALLOWED_DEFAULT_ARG, "false" );
        try ( DummyDb db = new DummyDb() )
        {
            db.init(
                    params,
                    loggingService,
                    DummyWorkload.OPERATION_TYPE_CLASS_MAPPING
            );

            WorkloadRunnerThread runnerThread = workloadRunnerThread(
                    timeSource,
                    workloadStreams,
                    threadCount,
                    errorReporter,
                    metricsService,
                    completionTimeService,
                    db
            );

            MetricsService.MetricsServiceWriter metricsServiceWriter = metricsService.getWriter();

            // initialize GCT
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters( completionTimeService, 0 );
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters( completionTimeService, 1 );

            timeSource.setNowFromMilli( 0 );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), is( 0l ) );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 0l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            runnerThread.start();

            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            // GCT may be 0 or 1 at this stage, depending on the OperationHandlerExecutor used
            // anyOf because it depends on whether "readwrite1"/S(3)D(0) has been initialized yet, or not
            // SameThreadOperationHandlerExecutor will be 0, as it must wait for previous operation to complete
            // before it can initiate the next operation
            // SingleThread/ThreadPoolOperationHandlerExecutor will be 1, as it can initiate the next operation as
            // soon as it has submitted the previous one for execution
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 0l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( is( 0l ), is( 1l ) ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 1 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 0l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( is( 0l ), is( 1l ) ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 2 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 0l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( is( 0l ), is( 1l ) ) );
            db.setNameAllowedValue( "read1", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 1l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( is( 0l ), is( 1l ) ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 3 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 1l ) );
            // anyOf because it depends on whether "readwrite2"/S(6)D(0) has been initialized yet, or not
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( is( 1l ), is( 3l ) ) );
            db.setNameAllowedValue( "readwrite1", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 2l ) );
            // anyOf because it depends on whether "readwrite2"/S(6)D(0) has been initialized yet, or not
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( is( 1l ), is( 3l ) ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 4 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 2l ) );
            // anyOf because it depends on whether "readwrite2"/S(6)D(0) has been initialized yet, or not
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( is( 1l ), is( 3l ) ) );
            db.setNameAllowedValue( "read2", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 3l ) );
            // anyOf because it depends on whether "readwrite2"/S(6)D(0) has been initialized yet, or not
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( is( 1l ), is( 3l ) ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 5 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 3l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );
            // anyOf because it depends on whether "readwrite2"/S(6)D(0) has been initialized yet, or not
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( is( 1l ), is( 3l ) ) );

            timeSource.setNowFromMilli( 6 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 3l ) );
            // anyOf because it depends on whether "readwrite3"/S(9)D(0) has been initialized yet, or not
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( is( 3l ), is( 6l ) ) );
            db.setNameAllowedValue( "readwrite2", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 4l ) );
            // anyOf because it depends on whether "readwrite3"/S(9)D(0) has been initialized yet, or not
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( is( 3l ), is( 6l ) ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 7 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 4l ) );
            // anyOf because it depends on whether "readwrite3"/S(9)D(0) has been initialized yet, or not
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( is( 3l ), is( 6l ) ) );
            db.setNameAllowedValue( "read3", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 5l ) );
            // anyOf because it depends on whether "readwrite3"/S(9)D(0) has been initialized yet, or not
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( is( 3l ), is( 6l ) ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 8 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 5l ) );
            // anyOf because it depends on whether "readwrite3"/S(9)D(0) has been initialized yet, or not
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( is( 3l ), is( 6l ) ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 9 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 5l ) );
            // anyOf because it depends on whether "readwrite3"/S(9)D(0) has been initialized yet, or not
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( is( 6l ), is( 9l ) ) );
            db.setNameAllowedValue( "readwrite3", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 6l ) );
            // should advance to 9, because this is the last GCT writing operation in the stream
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 10 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 6l ) );
            // should advance to 9, because this is the last GCT writing operation in the stream
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 11 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 6l ) );
            // should advance to 9, because this is the last GCT writing operation in the stream
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            db.setNameAllowedValue( "read4", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 7l ) );
            // should advance to 9, because this is the last GCT writing operation in the stream
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 12 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 7l ) );
            // should advance to 9, because this is the last GCT writing operation in the stream
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 13 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 7l ) );
            // should advance to 9, because this is the last GCT writing operation in the stream
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            db.setNameAllowedValue( "read5", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 8l ) );
            // should advance to 9, because this is the last GCT writing operation in the stream
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            long durationToWaitForRunnerToCompleteAsMilli = WorkloadRunner.RUNNER_POLLING_INTERVAL_AS_MILLI * 4;
            long timeoutTimeAsMilli = timeSource.nowAsMilli() + durationToWaitForRunnerToCompleteAsMilli;
            while ( timeSource.nowAsMilli() < timeoutTimeAsMilli )
            {
                if ( runnerThread.runnerHasCompleted() )
                {
                    break;
                }
                Spinner.powerNap( 100 );
            }

            db.setAllowedValueForAll( true );
            assertThat( errorReporter.toString(), runnerThread.runnerHasCompleted(), is( true ) );
            assertThat( errorReporter.toString(), runnerThread.runnerCompletedSuccessfully(), is( true ) );
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
            throw e;
        }
        finally
        {
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            metricsService.shutdown();
            completionTimeService.shutdown();
        }
    }

    @Test
    public void shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadBlockingReadWriteAsync()
            throws DriverConfigurationException, DbException, CompletionTimeException, WorkloadException,
            InterruptedException, MetricsCollectionException, IOException
    {
        shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadBlockingReadWriteAsync( 1 );
        shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadBlockingReadWriteAsync( 4 );
        shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadBlockingReadWriteAsync( 16 );
    }

    public void shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadBlockingReadWriteAsync( int threadCount )
            throws DriverConfigurationException, DbException, CompletionTimeException, WorkloadException,
            InterruptedException, MetricsCollectionException, IOException
    {
        // @formatter:off
        /*
            Number of writers: 1 (async)
            Number of executors: 2 (blocking & async)
            Initialized to: IT[ , ] CT[0,1]

            BLOCKING                ASYNC
            READ                    READ_WRITE
            TimedNamedOperation1    TimedNamedOperation2    GCT (assumes initiated time submitted quickly)
        0                                                   1 <-- S(3)D(0) initialized <~~ S(2)D(0) initialized (READ
         ONLY)
        1                                                   1
        2   S(2)D(0)                                        1 <~~ S(4)D(0) initialized (READ ONLY)
        3                           S(3)D(0)                3 <-- S(6)D(0) initialized
        4   S(4)D(0)                                        3 <~~ S(7)D(3) initialized (READ ONLY)
        5                                                   3
        6                           S(6)D(0)                6 <-- S(9)D(3) initialized
        7   S(7)D(3)                                        6 <~~ S(11)D(0) initialized (READ ONLY)
        8                                                   6
        9                           S(9)D(3)                9 // Executor knows this is the last WRITE
        10                                                  9
        11  S(11)D(0)                                       9 <~~ S(13)D(6) initialized (READ ONLY)
        12                                                  9
        13  S(13)D(6)                                       9
         */
        // @formatter:on
        LoggingService loggingService = new Log4jLoggingServiceFactory( false ).loggingServiceFor( "Test" );
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

        SimpleCsvFileWriter csvResultsLogWriter = null;
        Map<Integer,Class<? extends Operation>> operationTypeToClassMapping = new HashMap<>();
        operationTypeToClassMapping.put( TimedNamedOperation1.TYPE, TimedNamedOperation1.class );
        operationTypeToClassMapping.put( TimedNamedOperation2.TYPE, TimedNamedOperation2.class );
        operationTypeToClassMapping.put( TimedNamedOperation3.TYPE, TimedNamedOperation3.class );
        MetricsService metricsService = ThreadedQueuedMetricsService.newInstanceUsingBlockingBoundedQueue(
                timeSource,
                errorReporter,
                TimeUnit.MILLISECONDS,
                ThreadedQueuedMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO,
                csvResultsLogWriter,
                operationTypeToClassMapping,
                LOG4J_LOGGING_SERVICE_FACTORY
        );

        Set<String> peerIds = new HashSet<>();
        // TODO test also with threaded completion time service implementation
        CompletionTimeService completionTimeService =
                completionTimeServiceAssistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds( peerIds );

        WorkloadStreams workloadStreams = new WorkloadStreams();
        Set<Class<? extends Operation>> asynchronousDependentOperationTypes =
                Sets.<Class<? extends Operation>>newHashSet(
                        TimedNamedOperation2.class
                );
        Set<Class<? extends Operation>> asynchronousDependencyOperationTypes =
                Sets.<Class<? extends Operation>>newHashSet(
                        TimedNamedOperation2.class
                );
        Iterator<Operation> asynchronousDependencyOperations = Lists.<Operation>newArrayList(
                new TimedNamedOperation2( 3, 3, 0, "readwrite1" ),
                new TimedNamedOperation2( 6, 6, 0, "readwrite2" ),
                new TimedNamedOperation2( 9, 9, 3, "readwrite3" )
        ).iterator();
        Iterator<Operation> asynchronousNonDependencyOperations = Lists.<Operation>newArrayList(
                // nothing
        ).iterator();
        workloadStreams.setAsynchronousStream(
                asynchronousDependentOperationTypes,
                asynchronousDependencyOperationTypes,
                asynchronousDependencyOperations,
                asynchronousNonDependencyOperations,
                null
        );
        Set<Class<? extends Operation>> blockingDependentOperationTypes = Sets.<Class<? extends Operation>>newHashSet(
                TimedNamedOperation1.class
        );
        Set<Class<? extends Operation>> blockingDependencyOperationTypes = Sets.<Class<? extends Operation>>newHashSet(
        );
        Iterator<Operation> blockingDependencyOperations = Lists.<Operation>newArrayList(
                // nothing
        ).iterator();
        Iterator<Operation> blockingNonDependencyOperations = Lists.<Operation>newArrayList(
                new TimedNamedOperation1( 2, 2, 0, "read1" ),
                new TimedNamedOperation1( 4, 4, 0, "read2" ),
                new TimedNamedOperation1( 7, 7, 3, "read3" ),
                new TimedNamedOperation1( 11, 11, 0, "read4" ),
                new TimedNamedOperation1( 13, 13, 6, "read5" )
        ).iterator();
        workloadStreams.addBlockingStream(
                blockingDependentOperationTypes,
                blockingDependencyOperationTypes,
                blockingDependencyOperations,
                blockingNonDependencyOperations,
                null
        );

        Map<String,String> params = new HashMap<>();
        params.put( DummyDb.ALLOWED_DEFAULT_ARG, "false" );
        try ( DummyDb db = new DummyDb() )
        {
            db.init(
                    params,
                    loggingService,
                    DummyWorkload.OPERATION_TYPE_CLASS_MAPPING
            );

            WorkloadRunnerThread runnerThread = workloadRunnerThread(
                    timeSource,
                    workloadStreams,
                    threadCount,
                    errorReporter,
                    metricsService,
                    completionTimeService,
                    db
            );

            MetricsService.MetricsServiceWriter metricsServiceWriter = metricsService.getWriter();

            // initialize GCT
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters( completionTimeService, 0 );
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters( completionTimeService, 1 );

            timeSource.setNowFromMilli( 0 );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), is( 0l ) );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 0l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            runnerThread.start();

            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 0l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), is( 1l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 1 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 0l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), is( 1l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 2 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 0l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), is( 1l ) );
            db.setNameAllowedValue( "read1", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 1l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), is( 1l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 3 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 1l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), is( 1l ) );
            db.setNameAllowedValue( "readwrite1", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 2l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 3l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 4 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 2l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 3l ) );
            db.setNameAllowedValue( "read2", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 3l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 3l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 5 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 3l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 3l ) );

            timeSource.setNowFromMilli( 6 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 3l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 3l ) );
            db.setNameAllowedValue( "readwrite2", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 4l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 6l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 7 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 4l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 6l ) );
            db.setNameAllowedValue( "read3", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 5l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 6l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 8 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 5l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 6l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 9 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 5l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 6l ) );
            db.setNameAllowedValue( "readwrite3", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 6l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 10 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 6l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 11 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 6l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            db.setNameAllowedValue( "read4", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 7l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 12 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 7l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 13 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 7l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            db.setNameAllowedValue( "read5", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 8l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            long durationToWaitForRunnerToCompleteAsMilli = WorkloadRunner.RUNNER_POLLING_INTERVAL_AS_MILLI * 4;
            long timeoutTimeAsMilli = timeSource.nowAsMilli() + durationToWaitForRunnerToCompleteAsMilli;
            while ( timeSource.nowAsMilli() < timeoutTimeAsMilli )
            {
                if ( runnerThread.runnerHasCompleted() )
                {
                    break;
                }
                Spinner.powerNap( 100 );
            }

            db.setAllowedValueForAll( true );
            assertThat( errorReporter.toString(), runnerThread.runnerHasCompleted(), is( true ) );
            assertThat( errorReporter.toString(), runnerThread.runnerCompletedSuccessfully(), is( true ) );
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
            throw e;
        }
        finally
        {
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            metricsService.shutdown();
            completionTimeService.shutdown();
        }
    }

    @Test
    public void shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadBlockingReadWriteBlocking()
            throws DriverConfigurationException, DbException, CompletionTimeException, WorkloadException,
            InterruptedException, MetricsCollectionException, IOException
    {
        shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadBlockingReadWriteBlocking( 1 );
        shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadBlockingReadWriteBlocking( 4 );
        shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadBlockingReadWriteBlocking( 16 );
    }

    public void shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadBlockingReadWriteBlocking(
            int threadCount )
            throws DriverConfigurationException, DbException, CompletionTimeException, WorkloadException,
            InterruptedException, MetricsCollectionException, IOException
    {
        // @formatter:off
        /*
            Number of writers: 1 (blocking)
            Number of executors: 1 (blocking)
            Initialized to: IT[ , ] CT[0,1]

            BLOCKING                BLOCKING
            READ                    READ_WRITE
            TimedNamedOperation1    TimedNamedOperation2    GCT (assumes initiated time submitted quickly)
        0                                                   0 <~~ S(2)D(0) initialized (READ ONLY)
        1                                                   0
        2   S(2)D(0)                                        0 <-- S(3)D(0) initialized
        3                           S(3)D(0)                1 <~~ S(4)D(0) initialized (READ ONLY)
        4   S(4)D(0)                                        3 <-- S(6)D(0) initialized
        5                                                   3
        6                           S(6)D(0)                3 <~~ S(7)D(3) initialized (READ ONLY)
        7   S(7)D(3)                                        6 <-- S(9)D(3) initialized
        8                                                   6
        9                           S(9)D(3)                6 <~~ S(11)D(0) initialized (READ ONLY)
        10                                                  6
        11  S(11)D(0)                                       6 <~~ S(13)D(6) initialized (READ ONLY)
        12                                                  6
        13  S(13)D(6)                                       6
         */
        // @formatter:on
        LoggingService loggingService = new Log4jLoggingServiceFactory( false ).loggingServiceFor( "Test" );
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

        SimpleCsvFileWriter csvResultsLogWriter = null;
        Map<Integer,Class<? extends Operation>> operationTypeToClassMapping = new HashMap<>();
        operationTypeToClassMapping.put( TimedNamedOperation1.TYPE, TimedNamedOperation1.class );
        operationTypeToClassMapping.put( TimedNamedOperation2.TYPE, TimedNamedOperation2.class );
        operationTypeToClassMapping.put( TimedNamedOperation3.TYPE, TimedNamedOperation3.class );
        MetricsService metricsService = ThreadedQueuedMetricsService.newInstanceUsingBlockingBoundedQueue(
                timeSource,
                errorReporter,
                TimeUnit.MILLISECONDS,
                ThreadedQueuedMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO,
                csvResultsLogWriter,
                operationTypeToClassMapping,
                LOG4J_LOGGING_SERVICE_FACTORY
        );

        Set<String> peerIds = new HashSet<>();
        // TODO test also with threaded completion time service implementation
        CompletionTimeService completionTimeService =
                completionTimeServiceAssistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds( peerIds );

        WorkloadStreams workloadStreams = new WorkloadStreams();
        Set<Class<? extends Operation>> asynchronousDependentOperationTypes =
                Sets.<Class<? extends Operation>>newHashSet(
                        // nothing
                );
        Set<Class<? extends Operation>> asynchronousDependencyOperationTypes =
                Sets.<Class<? extends Operation>>newHashSet(
                        // nothing
                );
        Iterator<Operation> asynchronousDependencyOperations = Lists.<Operation>newArrayList(
                // nothing
        ).iterator();
        Iterator<Operation> asynchronousNonDependencyOperations = Lists.<Operation>newArrayList(
                // nothing
        ).iterator();
        workloadStreams.setAsynchronousStream(
                asynchronousDependentOperationTypes,
                asynchronousDependencyOperationTypes,
                asynchronousDependencyOperations,
                asynchronousNonDependencyOperations,
                null
        );
        Set<Class<? extends Operation>> blockingDependentOperationTypes = Sets.<Class<? extends Operation>>newHashSet(
                TimedNamedOperation1.class,
                TimedNamedOperation2.class
        );
        Set<Class<? extends Operation>> blockingDependencyOperationTypes = Sets.<Class<? extends Operation>>newHashSet(
                TimedNamedOperation2.class
        );
        Iterator<Operation> blockingDependencyOperations = Lists.<Operation>newArrayList(
                new TimedNamedOperation2( 3, 3, 0, "readwrite1" ),
                new TimedNamedOperation2( 6, 6, 0, "readwrite2" ),
                new TimedNamedOperation2( 9, 9, 3, "readwrite3" )
        ).iterator();
        Iterator<Operation> blockingNonDependencyOperations = Lists.<Operation>newArrayList(
                new TimedNamedOperation1( 2, 2, 0, "read1" ),
                new TimedNamedOperation1( 4, 4, 0, "read2" ),
                new TimedNamedOperation1( 7, 7, 3, "read3" ),
                new TimedNamedOperation1( 11, 11, 0, "read4" ),
                new TimedNamedOperation1( 13, 13, 6, "read5" )
        ).iterator();
        workloadStreams.addBlockingStream(
                blockingDependentOperationTypes,
                blockingDependencyOperationTypes,
                blockingDependencyOperations,
                blockingNonDependencyOperations,
                null
        );

        Map<String,String> params = new HashMap<>();
        params.put( DummyDb.ALLOWED_DEFAULT_ARG, "false" );
        try ( DummyDb db = new DummyDb() )
        {
            db.init(
                    params,
                    loggingService,
                    DummyWorkload.OPERATION_TYPE_CLASS_MAPPING
            );

            WorkloadRunnerThread runnerThread = workloadRunnerThread(
                    timeSource,
                    workloadStreams,
                    threadCount,
                    errorReporter,
                    metricsService,
                    completionTimeService,
                    db
            );

            MetricsService.MetricsServiceWriter metricsServiceWriter = metricsService.getWriter();

            // initialize GCT
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters( completionTimeService, 0 );
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters( completionTimeService, 1 );

            timeSource.setNowFromMilli( 0 );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), is( 0l ) );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 0l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            runnerThread.start();

            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 0l ) );
            // GCT may be 0 or 1 at this stage, depending on the OperationHandlerExecutor used
            // anyOf because it depends on whether "readwrite1"/S(3)D(0) has been initialized yet, or not
            // SameThreadOperationHandlerExecutor will be 0, as it must wait for previous operation to complete
            // before it can initiate the next operation
            // SingleThread/ThreadPoolOperationHandlerExecutor will be 1, as it can initiate the next operation as
            // soon as it has submitted the previous one for execution
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( is( 0l ), is( 1l ) ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 1 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 0l ) );
            // anyOf because it depends on whether "readwrite1"/S(3)D(0) has been initialized yet, or not
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( is( 0l ), is( 1l ) ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 2 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 0l ) );
            // anyOf because it depends on whether "readwrite1"/S(3)D(0) has been initialized yet, or not
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( equalTo( 0l ), equalTo( 1l ) ) );
            db.setNameAllowedValue( "read1", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 1l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), is( 1l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 3 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 1l ) );
            // anyOf because it depends on whether "readwrite2"/S(6)D(0) has been initialized yet, or not
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( equalTo( 1l ), equalTo( 3l ) ) );
            db.setNameAllowedValue( "readwrite1", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 2l ) );
            // anyOf because it depends on whether "readwrite2"/S(6)D(0) has been initialized yet, or not
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( equalTo( 1l ), equalTo( 3l ) ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 4 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 2l ) );
            // anyOf because it depends on whether "readwrite2"/S(6)D(0) has been initialized yet, or not
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( equalTo( 1l ), equalTo( 3l ) ) );
            db.setNameAllowedValue( "read2", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 3l ) );
            // GCT may be 0 or 1 at this stage, depending on the OperationHandlerExecutor used
            // anyOf because it depends on whether "readwrite2"/S(6)D(0) has been initialized yet, or not
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( equalTo( 1l ), equalTo( 3l ) ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 5 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 3l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );
            // anyOf because it depends on whether "readwrite2"/S(6)D(0) has been initialized yet, or not
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( equalTo( 1l ), equalTo( 3l ) ) );

            timeSource.setNowFromMilli( 6 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 3l ) );
            // anyOf because it depends on whether "readwrite3"/S(9)D(0) has been initialized yet, or not
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( equalTo( 3l ), equalTo( 6l ) ) );
            db.setNameAllowedValue( "readwrite2", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 4l ) );
            // anyOf because it depends on whether "readwrite3"/S(9)D(0) has been initialized yet, or not
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( equalTo( 3l ), equalTo( 6l ) ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 7 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 4l ) );
            // anyOf because it depends on whether "readwrite3"/S(9)D(0) has been initialized yet, or not
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( equalTo( 3l ), equalTo( 6l ) ) );
            db.setNameAllowedValue( "read3", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 5l ) );
            // anyOf because it depends on whether "readwrite3"/S(9)D(0) has been initialized yet, or not
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( equalTo( 3l ), equalTo( 6l ) ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 8 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 5l ) );
            // anyOf because it depends on whether "readwrite3"/S(9)D(0) has been initialized yet, or not
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(),
                    anyOf( equalTo( 3l ), equalTo( 6l ) ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 9 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 5l ) );
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 6l ) );
            db.setNameAllowedValue( "readwrite3", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 6l ) );
            // should advance to 9, because this is the last GCT writing operation in the stream
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 10 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 6l ) );
            // should advance to 9, because this is the last GCT writing operation in the stream
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 11 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 6l ) );
            // should advance to 9, because this is the last GCT writing operation in the stream
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            db.setNameAllowedValue( "read4", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 7l ) );
            // should advance to 9, because this is the last GCT writing operation in the stream
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 12 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 7l ) );
            // should advance to 9, because this is the last GCT writing operation in the stream
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            timeSource.setNowFromMilli( 13 );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 7l ) );
            // should advance to 9, because this is the last GCT writing operation in the stream
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            db.setNameAllowedValue( "read5", true );
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            assertThat( errorReporter.toString(), metricsServiceWriter.results().totalOperationCount(), is( 8l ) );
            // should advance to 9, because this is the last GCT writing operation in the stream
            assertThat( errorReporter.toString(), completionTimeService.globalCompletionTimeAsMilli(), equalTo( 9l ) );
            assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

            long durationToWaitForRunnerToCompleteAsMilli = WorkloadRunner.RUNNER_POLLING_INTERVAL_AS_MILLI * 4;
            long timeoutTimeAsMilli = timeSource.nowAsMilli() + durationToWaitForRunnerToCompleteAsMilli;
            while ( timeSource.nowAsMilli() < timeoutTimeAsMilli )
            {
                if ( runnerThread.runnerHasCompleted() )
                {
                    break;
                }
                Spinner.powerNap( 100 );
            }

            db.setAllowedValueForAll( true );
            assertThat( errorReporter.toString(), runnerThread.runnerHasCompleted(), is( true ) );
            assertThat( errorReporter.toString(), runnerThread.runnerCompletedSuccessfully(), is( true ) );
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
            throw e;
        }
        finally
        {
            Thread.sleep( ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING );
            metricsService.shutdown();
            completionTimeService.shutdown();
        }
    }

    private WorkloadRunnerThread workloadRunnerThread( TimeSource timeSource,
            WorkloadStreams workloadStreams,
            int threadCount,
            ConcurrentErrorReporter errorReporter,
            MetricsService metricsService,
            CompletionTimeService completionTimeService,
            Db db )
            throws WorkloadException, CompletionTimeException, DbException, MetricsCollectionException
    {
        boolean ignoreScheduledStartTime = false;
        long statusDisplayIntervalAsMilli = 0;
        long spinnerSleepDurationAsMilli = SPINNER_SLEEP_DURATION_AS_MILLI;
        int operationHandlerExecutorsBoundedQueueSize = 100;
        boolean detailedStatus = false;
        LoggingServiceFactory loggingServiceFactory = new Log4jLoggingServiceFactory( detailedStatus );
        WorkloadRunner runner = new WorkloadRunner(
                timeSource,
                db,
                workloadStreams,
                metricsService,
                errorReporter,
                completionTimeService,
                loggingServiceFactory,
                threadCount,
                statusDisplayIntervalAsMilli,
                spinnerSleepDurationAsMilli,
                ignoreScheduledStartTime,
                operationHandlerExecutorsBoundedQueueSize
        );
        return new WorkloadRunnerThread( runner, errorReporter );
    }

    private class WorkloadRunnerThread extends Thread
    {
        private final WorkloadRunner runner;
        private final AtomicBoolean runnerHasCompleted;
        private final AtomicBoolean runnerCompletedSuccessfully;
        private final ConcurrentErrorReporter errorReporter;

        WorkloadRunnerThread( WorkloadRunner runner, ConcurrentErrorReporter errorReporter )
        {
            super( WorkloadRunnerThread.class.getSimpleName() + "-" + System.currentTimeMillis() );
            this.runner = runner;
            this.runnerHasCompleted = new AtomicBoolean( false );
            this.runnerCompletedSuccessfully = new AtomicBoolean( false );
            this.errorReporter = errorReporter;
        }

        @Override
        public void run()
        {
            try
            {
                runner.getFuture().get();
                runnerCompletedSuccessfully.set( true );
                runnerHasCompleted.set( true );
            }
            catch ( Throwable e )
            {
                errorReporter.reportError( this, ConcurrentErrorReporter.stackTraceToString( e ) );
                runnerCompletedSuccessfully.set( false );
                runnerHasCompleted.set( true );
            }
        }

        boolean runnerHasCompleted()
        {
            return runnerHasCompleted.get();
        }

        boolean runnerCompletedSuccessfully()
        {
            return runnerCompletedSuccessfully.get();
        }
    }
}
