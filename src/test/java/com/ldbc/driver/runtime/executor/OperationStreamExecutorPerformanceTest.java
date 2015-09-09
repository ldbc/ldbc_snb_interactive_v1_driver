package com.ldbc.driver.runtime.executor;

import com.google.common.collect.Lists;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.control.Log4jLoggingServiceFactory;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.DefaultQueues;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.DummyGlobalCompletionTimeReader;
import com.ldbc.driver.runtime.coordination.DummyLocalCompletionTimeWriter;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.DummyCountingMetricsService;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.runtime.metrics.MetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.ManualTimeSource;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.workloads.dummy.DummyDb;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1Factory;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@Ignore
public class OperationStreamExecutorPerformanceTest
{
    private final ManualTimeSource timeSource = new ManualTimeSource( 0 );
    private final GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42l ) );

    @Test
    public void synchronousExecutorPerformanceTest()
            throws CompletionTimeException, MetricsCollectionException, DbException, OperationExecutorException,
            IOException
    {
        int experimentRepetitions;
        long operationCount;
        long spinnerSleepDuration;

        experimentRepetitions = 100;
        operationCount = 100000;
        spinnerSleepDuration = 0l;
        synchronousExecutorPerformanceTestWithSpinnerDuration( spinnerSleepDuration, experimentRepetitions,
                operationCount );

        experimentRepetitions = 100;
        operationCount = 100000;
        spinnerSleepDuration = 1l;
        synchronousExecutorPerformanceTestWithSpinnerDuration( spinnerSleepDuration, experimentRepetitions,
                operationCount );

        experimentRepetitions = 100;
        operationCount = 100000;
        spinnerSleepDuration = 10l;
        synchronousExecutorPerformanceTestWithSpinnerDuration( spinnerSleepDuration, experimentRepetitions,
                operationCount );
    }

    public void synchronousExecutorPerformanceTestWithSpinnerDuration( long spinnerSleepDuration,
            int experimentRepetitions, long operationCount )
            throws CompletionTimeException, MetricsCollectionException, DbException, OperationExecutorException,
            IOException
    {
        List<Long> threadPoolExecutorTimes = new ArrayList<>();
        List<Long> singleThreadExecutorTimes = new ArrayList<>();
        List<Long> sameThreadExecutorTimes = new ArrayList<>();

        List<Operation> operations = Lists.newArrayList( getOperations( operationCount ) );

        while ( experimentRepetitions-- > 0 )
        {
            // Thread Pool Executor
            {
                LoggingService loggingService = new Log4jLoggingServiceFactory( false ).loggingServiceFor( "Test" );
                boolean ignoreScheduledStartTime = false;
                ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
                Spinner spinner = new Spinner( timeSource, spinnerSleepDuration, ignoreScheduledStartTime );
                DummyDb db = new DummyDb();
                Map<String,String> dummyDbParameters = new HashMap<>();
                dummyDbParameters.put( DummyDb.ALLOWED_DEFAULT_ARG, Boolean.toString( true ) );
                db.init( dummyDbParameters, loggingService, new HashMap<Integer,Class<? extends Operation>>() );
                LocalCompletionTimeWriter localCompletionTimeWriter = new DummyLocalCompletionTimeWriter();
                MetricsService metricsService = new DummyCountingMetricsService();
                DummyGlobalCompletionTimeReader globalCompletionTimeReader = new DummyGlobalCompletionTimeReader();
                globalCompletionTimeReader.setGlobalCompletionTimeAsMilli( 0l );
                AtomicBoolean executorHasFinished = new AtomicBoolean( false );
                AtomicBoolean forceThreadToTerminate = new AtomicBoolean( false );
                timeSource.setNowFromMilli( 0 );

                WorkloadStreams.WorkloadStreamDefinition streamDefinition =
                        new WorkloadStreams.WorkloadStreamDefinition(
                                new HashSet<Class<? extends Operation>>(),
                                new HashSet<Class<? extends Operation>>(),
                                Collections.<Operation>emptyIterator(),
                                operations.iterator(),
                                null
                        );

                OperationExecutor executor = new ThreadPoolOperationExecutor(
                        1,
                        DefaultQueues.DEFAULT_BOUND_1000,
                        db,
                        streamDefinition,
                        localCompletionTimeWriter,
                        globalCompletionTimeReader,
                        spinner,
                        timeSource,
                        errorReporter,
                        metricsService,
                        streamDefinition.childOperationGenerator()
                );
                OperationStreamExecutorServiceThread thread = getNewThread(
                        errorReporter,
                        streamDefinition,
                        executor,
                        localCompletionTimeWriter,
                        executorHasFinished,
                        forceThreadToTerminate
                );

                threadPoolExecutorTimes.add( doTest( thread, errorReporter, metricsService, operationCount ) );
                executor.shutdown( 1000l );
                db.close();
                metricsService.shutdown();
            }
            // Single Thread Executor
            {
                LoggingService loggingService = new Log4jLoggingServiceFactory( false ).loggingServiceFor( "Test" );
                boolean ignoreScheduledStartTime = false;
                ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
                Spinner spinner = new Spinner( timeSource, spinnerSleepDuration, ignoreScheduledStartTime );
                DummyDb db = new DummyDb();
                Map<String,String> dummyDbParameters = new HashMap<>();
                dummyDbParameters.put( DummyDb.ALLOWED_DEFAULT_ARG, Boolean.toString( true ) );
                db.init( dummyDbParameters, loggingService, new HashMap<Integer,Class<? extends Operation>>() );
                LocalCompletionTimeWriter localCompletionTimeWriter = new DummyLocalCompletionTimeWriter();
                MetricsService metricsService = new DummyCountingMetricsService();
                DummyGlobalCompletionTimeReader globalCompletionTimeReader = new DummyGlobalCompletionTimeReader();
                globalCompletionTimeReader.setGlobalCompletionTimeAsMilli( 0l );
                AtomicBoolean executorHasFinished = new AtomicBoolean( false );
                AtomicBoolean forceThreadToTerminate = new AtomicBoolean( false );
                timeSource.setNowFromMilli( 0 );

                WorkloadStreams.WorkloadStreamDefinition streamDefinition =
                        new WorkloadStreams.WorkloadStreamDefinition(
                                new HashSet<Class<? extends Operation>>(),
                                new HashSet<Class<? extends Operation>>(),
                                Collections.<Operation>emptyIterator(),
                                operations.iterator(),
                                null
                        );

                OperationExecutor executor = new SingleThreadOperationExecutor(
                        db,
                        streamDefinition,
                        localCompletionTimeWriter,
                        globalCompletionTimeReader,
                        spinner,
                        timeSource,
                        errorReporter,
                        metricsService,
                        streamDefinition.childOperationGenerator(),
                        DefaultQueues.DEFAULT_BOUND_1000
                );
                OperationStreamExecutorServiceThread thread = getNewThread(
                        errorReporter,
                        streamDefinition,
                        executor,
                        localCompletionTimeWriter,
                        executorHasFinished,
                        forceThreadToTerminate
                );

                singleThreadExecutorTimes.add( doTest( thread, errorReporter, metricsService, operationCount ) );
                executor.shutdown( 1000l );
                db.close();
                metricsService.shutdown();
            }
            // Same Thread Executor
            {
                LoggingService loggingService = new Log4jLoggingServiceFactory( false ).loggingServiceFor( "Test" );
                boolean ignoreScheduledStartTime = false;
                ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
                Spinner spinner = new Spinner( timeSource, spinnerSleepDuration, ignoreScheduledStartTime );
                DummyDb db = new DummyDb();
                Map<String,String> dummyDbParameters = new HashMap<>();
                dummyDbParameters.put( DummyDb.ALLOWED_DEFAULT_ARG, Boolean.toString( true ) );
                db.init( dummyDbParameters, loggingService, new HashMap<Integer,Class<? extends Operation>>() );
                LocalCompletionTimeWriter localCompletionTimeWriter = new DummyLocalCompletionTimeWriter();
                MetricsService metricsService = new DummyCountingMetricsService();
                DummyGlobalCompletionTimeReader globalCompletionTimeReader = new DummyGlobalCompletionTimeReader();
                globalCompletionTimeReader.setGlobalCompletionTimeAsMilli( 0l );
                AtomicBoolean executorHasFinished = new AtomicBoolean( false );
                AtomicBoolean forceThreadToTerminate = new AtomicBoolean( false );
                timeSource.setNowFromMilli( 0 );

                WorkloadStreams.WorkloadStreamDefinition streamDefinition =
                        new WorkloadStreams.WorkloadStreamDefinition(
                                new HashSet<Class<? extends Operation>>(),
                                new HashSet<Class<? extends Operation>>(),
                                Collections.<Operation>emptyIterator(),
                                operations.iterator(),
                                null
                        );

                OperationExecutor executor = new SameThreadOperationExecutor(
                        db,
                        streamDefinition,
                        localCompletionTimeWriter,
                        globalCompletionTimeReader,
                        spinner,
                        timeSource,
                        errorReporter,
                        metricsService,
                        streamDefinition.childOperationGenerator()
                );
                OperationStreamExecutorServiceThread thread = getNewThread(
                        errorReporter,
                        streamDefinition,
                        executor,
                        localCompletionTimeWriter,
                        executorHasFinished,
                        forceThreadToTerminate
                );

                sameThreadExecutorTimes.add( doTest( thread, errorReporter, metricsService, operationCount ) );
                executor.shutdown( 1000l );
                db.close();
                metricsService.shutdown();
            }
        }

        long meanThreadPool = meanDuration( threadPoolExecutorTimes );
        System.out.println( format( "Spinner [Sleep = %s ms] (thread pool executor) %s ops in %s: %s ops/ms",
                spinnerSleepDuration, operationCount, meanThreadPool,
                (operationCount / (double) TimeUnit.MILLISECONDS.toNanos( meanThreadPool )) * 1000000 ) );
        long meanSingleThread = meanDuration( singleThreadExecutorTimes );
        System.out.println( format( "Spinner [Sleep = %s ms] (single thread executor) %s ops in %s: %s ops/ms",
                spinnerSleepDuration, operationCount, meanSingleThread,
                (operationCount / (double) TimeUnit.MILLISECONDS.toNanos( meanSingleThread )) * 1000000 ) );
        long meanSameThread = meanDuration( sameThreadExecutorTimes );
        System.out.println( format( "Spinner [Sleep = %s ms] (same thread executor) %s ops in %s: %s ops/ms",
                spinnerSleepDuration, operationCount, meanSameThread,
                (operationCount / (double) TimeUnit.MILLISECONDS.toNanos( meanSameThread )) * 1000000 ) );
        System.out.println();
    }

    private long meanDuration( List<Long> durations )
    {
        long totalAsMilli = 0;
        for ( Long duration : durations )
        {
            totalAsMilli += duration;
        }
        return totalAsMilli / durations.size();
    }

    private long doTest( Thread thread, ConcurrentErrorReporter errorReporter, MetricsService metricsService,
            long operationCount ) throws MetricsCollectionException
    {
        TimeSource systemTimeSource = new SystemTimeSource();
        long benchmarkStartTime = systemTimeSource.nowAsMilli();

        timeSource.setNowFromMilli( 1 );

        // Note, run() instead of start() to get more precise benchmark numbers
        thread.run();

        long benchmarkFinishTime = systemTimeSource.nowAsMilli();
        long benchmarkDuration = benchmarkFinishTime - benchmarkStartTime;

        assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

        // wait for all results to get processed by metrics service
        MetricsService.MetricsServiceWriter metricsServiceWriter = metricsService.getWriter();
        long metricsCollectionTimeoutAsMilli = systemTimeSource.nowAsMilli() + 2000;
        while ( systemTimeSource.nowAsMilli() < metricsCollectionTimeoutAsMilli &&
                metricsServiceWriter.results().totalOperationCount() < operationCount )
        {
            Spinner.powerNap( 500 );
        }
        long numberResultsCollected = metricsServiceWriter.results().totalOperationCount();
        assertThat( format( "%s of %s results collected by metrics service", numberResultsCollected,
                operationCount ), numberResultsCollected, is( operationCount ) );

        return benchmarkDuration;
    }

    private Iterator<Operation> getOperations( long count )
    {
        Iterator<Long> scheduledStartTimes = gf.constant( 1l );
        Iterator<Long> dependencyTimes = gf.constant( 0l );
        Iterator<String> names = gf.constant( "name" );
        Iterator<Operation> operations =
                gf.limit( new TimedNamedOperation1Factory( scheduledStartTimes, dependencyTimes, names ), count );
        return operations;
    }

    private OperationStreamExecutorServiceThread getNewThread(
            ConcurrentErrorReporter errorReporter,
            WorkloadStreams.WorkloadStreamDefinition streamDefinition,
            OperationExecutor operationExecutor,
            LocalCompletionTimeWriter localCompletionTimeWriter,
            AtomicBoolean executorHasFinished,
            AtomicBoolean forceThreadToTerminate
    ) throws CompletionTimeException, MetricsCollectionException, DbException
    {
        OperationStreamExecutorServiceThread operationStreamExecutorThread =
                new OperationStreamExecutorServiceThread(
                        operationExecutor,
                        errorReporter,
                        streamDefinition,
                        executorHasFinished,
                        forceThreadToTerminate,
                        localCompletionTimeWriter
                );

        return operationStreamExecutorThread;
    }
}
