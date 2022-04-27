package org.ldbcouncil.snb.driver.runtime.executor;

import org.ldbcouncil.snb.driver.Db;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.WorkloadStreams;
import org.ldbcouncil.snb.driver.control.Log4jLoggingServiceFactory;
import org.ldbcouncil.snb.driver.control.LoggingService;
import org.ldbcouncil.snb.driver.runtime.ConcurrentErrorReporter;
import org.ldbcouncil.snb.driver.runtime.DefaultQueues;
import org.ldbcouncil.snb.driver.runtime.coordination.CompletionTimeWriter;
import org.ldbcouncil.snb.driver.runtime.coordination.DummyCompletionTimeWriter;
import org.ldbcouncil.snb.driver.runtime.coordination.DummyCompletionTimeReader;
import org.ldbcouncil.snb.driver.runtime.metrics.DummyCountingMetricsService;
import org.ldbcouncil.snb.driver.runtime.scheduling.Spinner;
import org.ldbcouncil.snb.driver.temporal.SystemTimeSource;
import org.ldbcouncil.snb.driver.temporal.TimeSource;
import org.ldbcouncil.snb.driver.workloads.dummy.DummyDb;
import org.ldbcouncil.snb.driver.workloads.dummy.DummyWorkload;
import org.ldbcouncil.snb.driver.workloads.dummy.NothingOperation;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ThreadPoolOperationExecutorTest
{
    @Test
    public void executorShouldReturnExpectedResult() throws Exception
    {
        // Given
        LoggingService loggingService = new Log4jLoggingServiceFactory( false ).loggingServiceFor( "Test" );
        TimeSource timeSource = new SystemTimeSource();
        boolean ignoreScheduledStartTime = false;
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        Spinner spinner = new Spinner( timeSource, Spinner.DEFAULT_SLEEP_DURATION_10_MILLI, ignoreScheduledStartTime );
        CompletionTimeWriter dummyCompletionTimeWriter = new DummyCompletionTimeWriter();
        DummyCompletionTimeReader dummyCompletionTimeReader = new DummyCompletionTimeReader();
        dummyCompletionTimeReader.setCompletionTimeAsMilli( Long.MAX_VALUE );
        DummyCountingMetricsService metricsService = new DummyCountingMetricsService();
        WorkloadStreams.WorkloadStreamDefinition streamDefinition = new WorkloadStreams.WorkloadStreamDefinition(
                new HashSet<Class<? extends Operation>>(),
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                Collections.<Operation>emptyIterator(),
                null
        );
        Db db = new DummyDb();
        db.init(
                new HashMap<String,String>(),
                loggingService,
                DummyWorkload.OPERATION_TYPE_CLASS_MAPPING
        );

        int threadCount = 1;
        int boundedQueueSize = DefaultQueues.DEFAULT_BOUND_1000;

        OperationExecutor executor = new ThreadPoolOperationExecutor(
                threadCount,
                boundedQueueSize,
                db,
                streamDefinition,
                dummyCompletionTimeWriter,
                dummyCompletionTimeReader,
                spinner,
                timeSource,
                errorReporter,
                metricsService,
                streamDefinition.childOperationGenerator()
        );

        Operation operation = new NothingOperation();
        operation.setScheduledStartTimeAsMilli( timeSource.nowAsMilli() + 200 );
        operation.setTimeStamp( timeSource.nowAsMilli() + 200 );
        operation.setDependencyTimeStamp( 0l );

        // When
        executor.execute( operation );

        while ( executor.uncompletedOperationHandlerCount() > 0 )
        {
            // wait for handler to finish
            Spinner.powerNap( 100 );
        }

        // Then
        assertThat( metricsService.count(), is( 1l ) );
        executor.shutdown( 1000l );
        assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );
    }


    @Test
    public void executorShouldReturnAllResults() throws Exception
    {
        // Given
        LoggingService loggingService = new Log4jLoggingServiceFactory( false ).loggingServiceFor( "Test" );
        TimeSource timeSource = new SystemTimeSource();
        boolean ignoreScheduledStartTime = false;
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        Spinner spinner = new Spinner( timeSource, Spinner.DEFAULT_SLEEP_DURATION_10_MILLI, ignoreScheduledStartTime );
        CompletionTimeWriter dummyCompletionTimeWriter = new DummyCompletionTimeWriter();
        DummyCompletionTimeReader dummyCompletionTimeReader = new DummyCompletionTimeReader();
        dummyCompletionTimeReader.setCompletionTimeAsMilli( Long.MAX_VALUE );
        DummyCountingMetricsService metricsService = new DummyCountingMetricsService();
        WorkloadStreams.WorkloadStreamDefinition streamDefinition = new WorkloadStreams.WorkloadStreamDefinition(
                new HashSet<Class<? extends Operation>>(),
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                Collections.<Operation>emptyIterator(),
                null
        );
        Db db = new DummyDb();
        db.init(
                new HashMap<String,String>(),
                loggingService,
                DummyWorkload.OPERATION_TYPE_CLASS_MAPPING
        );

        int threadCount = 1;
        int boundedQueueSize = DefaultQueues.DEFAULT_BOUND_1000;

        OperationExecutor executor = new ThreadPoolOperationExecutor(
                threadCount,
                boundedQueueSize,
                db,
                streamDefinition,
                dummyCompletionTimeWriter,
                dummyCompletionTimeReader,
                spinner,
                timeSource,
                errorReporter,
                metricsService,
                streamDefinition.childOperationGenerator()
        );

        Operation operation1 = new NothingOperation();
        operation1.setScheduledStartTimeAsMilli( timeSource.nowAsMilli() + 100l );
        operation1.setTimeStamp( operation1.scheduledStartTimeAsMilli() );
        operation1.setDependencyTimeStamp( 0l );

        Operation operation2 = new NothingOperation();
        operation2.setScheduledStartTimeAsMilli( operation1.scheduledStartTimeAsMilli() + 100l );
        operation2.setTimeStamp( operation2.scheduledStartTimeAsMilli() );
        operation2.setDependencyTimeStamp( 0l );

        // When

        executor.execute( operation1 );
        executor.execute( operation2 );

        while ( executor.uncompletedOperationHandlerCount() > 0 )
        {
            // wait for handler to finish
            Spinner.powerNap( 100 );
        }

        // Then
        assertThat( metricsService.count(), is( 2l ) );
        executor.shutdown( 1000l );
        assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );
    }

    @Test
    public void executorShouldThrowExceptionIfShutdownMultipleTimes() throws Exception
    {
        // Given
        LoggingService loggingService = new Log4jLoggingServiceFactory( false ).loggingServiceFor( "Test" );
        TimeSource timeSource = new SystemTimeSource();
        boolean ignoreScheduledStartTime = false;
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        Spinner spinner = new Spinner( timeSource, Spinner.DEFAULT_SLEEP_DURATION_10_MILLI, ignoreScheduledStartTime );
        CompletionTimeWriter dummyCompletionTimeWriter = new DummyCompletionTimeWriter();
        DummyCompletionTimeReader dummyCompletionTimeReader = new DummyCompletionTimeReader();
        dummyCompletionTimeReader.setCompletionTimeAsMilli( Long.MAX_VALUE );
        DummyCountingMetricsService metricsService = new DummyCountingMetricsService();
        WorkloadStreams.WorkloadStreamDefinition streamDefinition = new WorkloadStreams.WorkloadStreamDefinition(
                new HashSet<Class<? extends Operation>>(),
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                Collections.<Operation>emptyIterator(),
                null
        );
        Db db = new DummyDb();
        db.init(
                new HashMap<String,String>(),
                loggingService,
                DummyWorkload.OPERATION_TYPE_CLASS_MAPPING
        );

        int threadCount = 1;
        int boundedQueueSize = DefaultQueues.DEFAULT_BOUND_1000;

        OperationExecutor executor = new ThreadPoolOperationExecutor(
                threadCount,
                boundedQueueSize,
                db,
                streamDefinition,
                dummyCompletionTimeWriter,
                dummyCompletionTimeReader,
                spinner,
                timeSource,
                errorReporter,
                metricsService,
                streamDefinition.childOperationGenerator()
        );

        Operation operation = new NothingOperation();
        operation.setScheduledStartTimeAsMilli( timeSource.nowAsMilli() + 200l );
        operation.setTimeStamp( timeSource.nowAsMilli() + 200l );
        operation.setDependencyTimeStamp( 0l );

        // When
        executor.execute( operation );

        while ( executor.uncompletedOperationHandlerCount() > 0 )
        {
            // wait for handler to finish
            Spinner.powerNap( 100 );
        }

        // Then
        assertThat( metricsService.count(), is( 1l ) );
        executor.shutdown( 1000l );
        assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );

        boolean exceptionThrown = false;
        try
        {
            executor.shutdown( 1000l );
        }
        catch ( OperationExecutorException e )
        {
            exceptionThrown = true;
        }

        assertThat( exceptionThrown, is( true ) );
        assertThat( errorReporter.toString(), errorReporter.errorEncountered(), is( false ) );
    }
}
