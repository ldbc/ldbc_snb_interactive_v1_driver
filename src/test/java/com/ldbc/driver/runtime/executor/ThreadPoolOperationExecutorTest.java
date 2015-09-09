package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.Db;
import com.ldbc.driver.Operation;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.control.Log4jLoggingServiceFactory;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.DefaultQueues;
import com.ldbc.driver.runtime.coordination.DummyGlobalCompletionTimeReader;
import com.ldbc.driver.runtime.coordination.DummyLocalCompletionTimeWriter;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.DummyCountingMetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.workloads.dummy.DummyDb;
import com.ldbc.driver.workloads.dummy.DummyWorkload;
import com.ldbc.driver.workloads.dummy.NothingOperation;
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
        LocalCompletionTimeWriter dummyLocalCompletionTimeWriter = new DummyLocalCompletionTimeWriter();
        DummyGlobalCompletionTimeReader dummyGlobalCompletionTimeReader = new DummyGlobalCompletionTimeReader();
        dummyGlobalCompletionTimeReader.setGlobalCompletionTimeAsMilli( Long.MAX_VALUE );
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
                dummyLocalCompletionTimeWriter,
                dummyGlobalCompletionTimeReader,
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
        LocalCompletionTimeWriter dummyLocalCompletionTimeWriter = new DummyLocalCompletionTimeWriter();
        DummyGlobalCompletionTimeReader dummyGlobalCompletionTimeReader = new DummyGlobalCompletionTimeReader();
        dummyGlobalCompletionTimeReader.setGlobalCompletionTimeAsMilli( Long.MAX_VALUE );
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
                dummyLocalCompletionTimeWriter,
                dummyGlobalCompletionTimeReader,
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
        LocalCompletionTimeWriter dummyLocalCompletionTimeWriter = new DummyLocalCompletionTimeWriter();
        DummyGlobalCompletionTimeReader dummyGlobalCompletionTimeReader = new DummyGlobalCompletionTimeReader();
        dummyGlobalCompletionTimeReader.setGlobalCompletionTimeAsMilli( Long.MAX_VALUE );
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
                dummyLocalCompletionTimeWriter,
                dummyGlobalCompletionTimeReader,
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
