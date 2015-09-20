package com.ldbc.driver.runtime;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ConcurrentErrorReporterTest
{
    @Test
    public void shouldMaintainOrderWhenErrorReportsAreSequential() throws InterruptedException, ExecutionException
    {
        // Given
        ConcurrentErrorReporter concurrentErrorReporter = new ConcurrentErrorReporter();

        // When
        concurrentErrorReporter.reportError( this, "Error 1" );
        concurrentErrorReporter.reportError( this, "Error 2" );
        concurrentErrorReporter.reportError( this, "Error 3" );

        // Then
//        System.out.println(concurrentErrorReporter.toString());
        Iterator<ConcurrentErrorReporter.ErrorReport> errorMessages =
                concurrentErrorReporter.errorMessages().iterator();
        assertThat( errorMessages.next().error(), is( "Error 1" ) );
        assertThat( errorMessages.next().error(), is( "Error 2" ) );
        assertThat( errorMessages.next().error(), is( "Error 3" ) );
        assertThat( concurrentErrorReporter.errorEncountered(), is( true ) );
        System.out.println( concurrentErrorReporter.toString() );
    }

    @Test
    public void shouldReportAllWhenErrorReportsAreConcurrent() throws InterruptedException, ExecutionException
    {
        // Given
        int taskCount = 100;
        int threadCount = 10;
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        ExecutorService threadPoolExecutorService = Executors.newFixedThreadPool( threadCount, threadFactory );
        CompletionService<Integer> completionService =
                new ExecutorCompletionService<Integer>( threadPoolExecutorService );
        ConcurrentErrorReporter concurrentErrorReporter = new ConcurrentErrorReporter();

        // When
        for ( int i = 0; i < taskCount; i++ )
        {
            completionService.submit( new ErrorReportingTask( concurrentErrorReporter, i ) );
        }

        threadPoolExecutorService.shutdown();
        threadPoolExecutorService.awaitTermination( 10, TimeUnit.SECONDS );

        // Then
        assertThat( Lists.newArrayList( concurrentErrorReporter.errorMessages() ).size(), is( taskCount ) );
        assertThat( concurrentErrorReporter.errorEncountered(), is( true ) );
        List<String> errorMessages =
                ImmutableList.copyOf(
                        Iterables.transform( concurrentErrorReporter.errorMessages(),
                                new Function<ConcurrentErrorReporter.ErrorReport,String>()
                                {
                                    @Override
                                    public String apply( ConcurrentErrorReporter.ErrorReport input )
                                    {
                                        return input.error();
                                    }
                                } ) );
        for ( int i = 0; i < taskCount; i++ )
        {
            assertThat( errorMessages.contains( format( "MyID=%s", i ) ), is( true ) );
        }
        System.out.println( concurrentErrorReporter.toString() );
    }

    class ErrorReportingTask implements Callable<Integer>
    {
        private final ConcurrentErrorReporter concurrentErrorReporter;
        private final int myId;

        public ErrorReportingTask( ConcurrentErrorReporter concurrentErrorReporter, int id )
        {
            this.concurrentErrorReporter = concurrentErrorReporter;
            this.myId = id;
        }

        public Integer call() throws Exception
        {
            try
            {
                String message = ((Integer) (null)).toString();
                System.out.printf( message );
            }
            catch ( Throwable e )
            {
                concurrentErrorReporter.reportError( this, format( "MyID=%s", myId ) );
            }
            return 0;
        }
    }
}