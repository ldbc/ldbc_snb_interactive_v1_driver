package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class CompletionTimeServiceBasicTest
{
    @Test
    public void shouldBehavePredictablyAfterInstantiationWithSynchronizedImplementation()
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts = assistant.newSynchronizedConcurrentCompletionTimeService();

        // Then
        try
        {
            shouldBehavePredictablyAfterInstantiation( cts );
        }
        finally
        {
            cts.shutdown();
        }
    }

    @Test
    public void shouldBehavePredictablyAfterInstantiationWithThreadedImplementation()
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts =
                assistant.newThreadedQueuedConcurrentCompletionTimeService( timeSource, errorReporter );

        // Then
        try
        {
            shouldBehavePredictablyAfterInstantiation( cts );
        }
        finally
        {
            cts.shutdown();
        }
    }

    private void shouldBehavePredictablyAfterInstantiation( CompletionTimeService cts )
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        // instantiated completion time service

        // When
        // nothing

        // Then
        assertThat( cts.globalCompletionTimeAsMilli(), is( -1L ) );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( -1L ) );
    }

    @Test
    public void shouldAdvanceGctWhenWriterSubmitInitiatedAndCompletedTimesWithSynchronizedImplementation()
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts = assistant.newSynchronizedConcurrentCompletionTimeService();

        // Then
        try
        {
            shouldAdvanceGctWhenWriterSubmitInitiatedAndCompletedTimes( cts );
        }
        finally
        {
            cts.shutdown();
        }
    }

    @Test
    public void shouldAdvanceGctWhenWriterSubmitInitiatedAndCompletedTimesWithThreadedImplementation()
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts =
                assistant.newThreadedQueuedConcurrentCompletionTimeService( timeSource, errorReporter );

        // Then
        try
        {
            shouldAdvanceGctWhenWriterSubmitInitiatedAndCompletedTimes( cts );
        }
        finally
        {
            cts.shutdown();
        }
    }

    private void shouldAdvanceGctWhenWriterSubmitInitiatedAndCompletedTimes(
            CompletionTimeService cts )
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        LocalCompletionTimeWriter writer1 = cts.newLocalCompletionTimeWriter();
        LocalCompletionTimeWriter writer2 = cts.newLocalCompletionTimeWriter();

        // When
        writer1.submitLocalInitiatedTime( 0L );
        writer1.submitLocalCompletedTime( 0L );
        writer2.submitLocalInitiatedTime( 0L );
        writer2.submitLocalCompletedTime( 0L );

        writer1.submitLocalInitiatedTime( 1L );
        writer2.submitLocalInitiatedTime( 1L );

        // Then
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 0L ) );
        assertThat( cts.globalCompletionTimeAsMilli(), is( 0L ) );
    }

    @Test
    public void shouldReturnAllWritersWithSynchronizedImplementation() throws CompletionTimeException
    {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts = assistant.newSynchronizedConcurrentCompletionTimeService();

        // Then
        try
        {
            shouldReturnAllWriters( cts );
        }
        finally
        {
            cts.shutdown();
        }
    }

    @Test
    public void shouldReturnAllWritersWithThreadedImplementation() throws CompletionTimeException
    {
        // Given
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts =
                assistant.newThreadedQueuedConcurrentCompletionTimeService( timeSource, errorReporter );

        // Then
        try
        {
            shouldReturnAllWriters( cts );
        }
        finally
        {
            cts.shutdown();
        }
    }

    private void shouldReturnAllWriters( CompletionTimeService cts ) throws CompletionTimeException
    {
        // Given
        // instantiated completion time service

        // When/Then
        assertThat( cts.getAllWriters().size(), is( 0 ) );

        LocalCompletionTimeWriter writer1 = cts.newLocalCompletionTimeWriter();

        assertThat( cts.getAllWriters().size(), is( 1 ) );
        assertThat( cts.getAllWriters().contains( writer1 ), is( true ) );

        LocalCompletionTimeWriter writer2 = cts.newLocalCompletionTimeWriter();
        LocalCompletionTimeWriter writer3 = cts.newLocalCompletionTimeWriter();

        assertThat( cts.getAllWriters().size(), is( 3 ) );
        assertThat( cts.getAllWriters().contains( writer1 ), is( true ) );
        assertThat( cts.getAllWriters().contains( writer2 ), is( true ) );
        assertThat( cts.getAllWriters().contains( writer3 ), is( true ) );
    }

    @Test
    public void shouldReturnNullWhenNoLocalITNoLocalCTWithSynchronizedImplementation()
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts = assistant.newSynchronizedConcurrentCompletionTimeService();

        // Then
        try
        {
            doShouldReturnNullWhenNoLocalITNoLocalCT( cts );
        }
        finally
        {
            cts.shutdown();
        }
    }

    @Test
    public void shouldReturnNullWhenNoLocalITNoLocalCTWithThreadedImplementation()
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts =
                assistant.newThreadedQueuedConcurrentCompletionTimeService( timeSource, errorReporter );

        // Then
        try
        {
            doShouldReturnNullWhenNoLocalITNoLocalCT( cts );
        }
        finally
        {
            cts.shutdown();
        }
    }

    // LocalIT = none, LocalCT = none --> null
    private void doShouldReturnNullWhenNoLocalITNoLocalCT( CompletionTimeService cts )
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given

        // When
        // no events have been initiated or completed

        // Then
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( -1L ) );
    }

    @Test
    public void shouldReturnNullWhenSomeITAndNoCTWithSynchronizedImplementation()
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts = assistant.newSynchronizedConcurrentCompletionTimeService();

        // Then
        try
        {
            doShouldReturnNullWhenSomeITAndNoCT( cts );
        }
        finally
        {
            cts.shutdown();
        }
    }

    @Test
    public void shouldReturnNullWhenSomeITAndNoCTWithThreadedImplementation()
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts =
                assistant.newThreadedQueuedConcurrentCompletionTimeService( timeSource, errorReporter );

        // Then
        try
        {
            doShouldReturnNullWhenSomeITAndNoCT( cts );
        }
        finally
        {
            cts.shutdown();
        }
    }

    // LocalIT = some, LocalCT = none --> null
    private void doShouldReturnNullWhenSomeITAndNoCT( CompletionTimeService cts )
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        LocalCompletionTimeWriter localCompletionTimeWriter = cts.newLocalCompletionTimeWriter();

        // When
        localCompletionTimeWriter.submitLocalInitiatedTime( 1000L );

        // Then
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( -1L ) );
    }

    @Test
    public void shouldReturnNullWhenSomeLocalITAndSomeLocalCTWithSynchronizedImplementation()
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts = assistant.newSynchronizedConcurrentCompletionTimeService();

        // Then
        try
        {
            doShouldReturnNullWhenSomeLocalITAndSomeLocalCT( cts );
        }
        finally
        {
            cts.shutdown();
        }
    }

    @Test
    public void shouldReturnNullWhenSomeLocalITAndSomeLocalCTWithThreadedImplementation()
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts =
                assistant.newThreadedQueuedConcurrentCompletionTimeService( timeSource, errorReporter );

        // Then
        try
        {
            doShouldReturnNullWhenSomeLocalITAndSomeLocalCT( cts );
        }
        finally
        {
            cts.shutdown();
        }
    }

    //  LocalIT = some, LocalCT = some --> null
    private void doShouldReturnNullWhenSomeLocalITAndSomeLocalCT( CompletionTimeService cts )
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        LocalCompletionTimeWriter localCompletionTimeWriter = cts.newLocalCompletionTimeWriter();

        // When
        localCompletionTimeWriter.submitLocalInitiatedTime( 1000L );
        localCompletionTimeWriter.submitLocalCompletedTime( 1000L );

        // Then
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( -1L ) );
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWithSynchronizedImplementation()
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts =
                assistant.newSynchronizedConcurrentCompletionTimeService();

        // Then
        try
        {
            doShouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWhen( cts );
        }
        finally
        {
            cts.shutdown();
        }
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWithThreadedImplementation()
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts =
                assistant.newThreadedQueuedConcurrentCompletionTimeService( timeSource, errorReporter );

        // Then
        try
        {
            doShouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWhen( cts );
        }
        finally
        {
            cts.shutdown();
        }
    }

    private void doShouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWhen( CompletionTimeService cts )
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        LocalCompletionTimeWriter localCompletionTimeWriter = cts.newLocalCompletionTimeWriter();

        // When/Then
        // initiated [1]
        // completed []
        localCompletionTimeWriter.submitLocalInitiatedTime( 1000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( -1L ) );

        // initiated [1]
        // completed [1]
        localCompletionTimeWriter.submitLocalCompletedTime( 1000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( -1L ) );

        // initiated [1,2]
        // completed [1]
        localCompletionTimeWriter.submitLocalInitiatedTime( 2000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 1000L
        ) );

        // initiated [1,2]
        // completed [1,2]
        localCompletionTimeWriter.submitLocalCompletedTime( 2000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 1000L
        ) );

        // initiated [1,2,3]
        // completed [1,2]
        localCompletionTimeWriter.submitLocalInitiatedTime( 3000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 2000L
        ) );

        // initiated [1,2,3,4]
        // completed [1,2]
        localCompletionTimeWriter.submitLocalInitiatedTime( 4000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 2000L
        ) );

        // initiated [1,2,3,4,5]
        // completed [1,2]
        localCompletionTimeWriter.submitLocalInitiatedTime( 5000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 2000L
        ) );

        // initiated [1,2,3,4,5]
        // completed [1,2, , ,5]
        localCompletionTimeWriter.submitLocalCompletedTime( 5000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 2000L
        ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, , , , ,  ]
        localCompletionTimeWriter.submitLocalInitiatedTime( 6000L );
        localCompletionTimeWriter.submitLocalInitiatedTime( 7000L );
        localCompletionTimeWriter.submitLocalInitiatedTime( 8000L );
        localCompletionTimeWriter.submitLocalInitiatedTime( 9000L );
        localCompletionTimeWriter.submitLocalInitiatedTime( 10000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 2000L
        ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, , ,8, ,  ]
        localCompletionTimeWriter.submitLocalCompletedTime( 8000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 2000L
        ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, ,7,8, ,  ]
        localCompletionTimeWriter.submitLocalCompletedTime( 7000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 2000L
        ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, ,7,8,9,  ]
        localCompletionTimeWriter.submitLocalCompletedTime( 9000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 2000L
        ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, ,4,5, ,7,8,9,  ]
        localCompletionTimeWriter.submitLocalCompletedTime( 4000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 2000L
        ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2,3,4,5, ,7,8,9,  ]
        localCompletionTimeWriter.submitLocalCompletedTime( 3000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 5000L
        ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2,3,4,5,6,7,8,9,  ]
        localCompletionTimeWriter.submitLocalCompletedTime( 6000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 9000L
        ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2,3,4,5,6,7,8,9,10]
        localCompletionTimeWriter.submitLocalCompletedTime( 10000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 9000L
        ) );

        // initiated [1,2,3,4,5,6,7,8,9,10,11]
        // completed [1,2,3,4,5,6,7,8,9,10,  ]
        localCompletionTimeWriter.submitLocalInitiatedTime( 11000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ),
                is( 10000L ) );
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWithDuplicateTimesWithSynchronizedImplementation()
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts = assistant.newSynchronizedConcurrentCompletionTimeService();

        // Then
        try
        {
            doShouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWithDuplicateTimes( cts );
        }
        finally
        {
            cts.shutdown();
        }
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWithDuplicateTimesWithThreadedImplementation()
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts =
                assistant.newThreadedQueuedConcurrentCompletionTimeService( timeSource, errorReporter );

        // Then
        try
        {
            doShouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWithDuplicateTimes( cts );
        }
        finally
        {
            try
            {
                cts.shutdown();
            }
            catch ( Throwable e )
            {
                // do nothing, exception is expected because test was trying to force an error
            }
        }
    }

    private void doShouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWithDuplicateTimes( CompletionTimeService cts )
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        LocalCompletionTimeWriter lctWriter = cts.newLocalCompletionTimeWriter();

        // When/Then
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( -1L ) );

        // initiated [1]
        // completed [1]
        lctWriter.submitLocalInitiatedTime( 1000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( -1L ) );

        lctWriter.submitLocalCompletedTime( 1000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( -1L ) );

        // initiated [1]
        // completed [1]
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( -1L ) );

        // initiated [1]
        // completed [1]
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( -1L ) );

        // initiated [1,2]
        // completed [1]
        lctWriter.submitLocalInitiatedTime( 2000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 1000L ) );

        // initiated [1,2,3]
        // completed [1]
        lctWriter.submitLocalInitiatedTime( 3000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 1000L ) );

        // initiated [1,2,3,4]
        // completed [1]
        lctWriter.submitLocalInitiatedTime( 4000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 1000L ) );

        // initiated [1,2,3,4]
        // completed [1, , ,4]
        lctWriter.submitLocalCompletedTime( 4000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 1000L ) );

        // initiated [1,2,3,4]
        // completed [1,2, ,4]
        lctWriter.submitLocalCompletedTime( 2000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 2000L ) );

        // initiated [1,2,3,4,5]
        // completed [1,2, ,4, ]
        lctWriter.submitLocalInitiatedTime( 5000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 2000L ) );

        // initiated [1,2,3,4,5]
        // completed [1,2, ,4, ]
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 2000L ) );

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4, ]
        lctWriter.submitLocalCompletedTime( 3000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 4000L ) );

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4,5]
        lctWriter.submitLocalCompletedTime( 5000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 4000L ) );

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4,5]
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 4000L ) );

        // initiated [1,2,3,4,5,6]
        // completed [1,2,3,4,5]
        lctWriter.submitLocalInitiatedTime( 6000L );
        assertThat( cts.globalCompletionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 5000L ) );
    }
}
