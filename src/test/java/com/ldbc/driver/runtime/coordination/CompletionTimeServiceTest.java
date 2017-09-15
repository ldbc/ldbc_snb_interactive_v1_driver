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

public class CompletionTimeServiceTest
{
    @Test
    public void shouldBehavePredictablyAfterInstantiationWithSynchronizedImplementation()
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts = assistant.newSynchronizedCompletionTimeService();

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
        CompletionTimeService cts = assistant.newThreadedQueuedCompletionTimeService( timeSource, errorReporter );

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
        assertThat( cts.completionTimeAsMilli(), is( -1L ) );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( -1L ) );
    }

    @Test
    public void shouldAdvanceCtWhenWriterSubmitInitiatedAndCompletedTimesWithSynchronizedImplementation()
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts = assistant.newSynchronizedCompletionTimeService();

        // Then
        try
        {
            shouldAdvanceCtWhenWriterSubmitInitiatedAndCompletedTimes( cts );
        }
        finally
        {
            cts.shutdown();
        }
    }

    @Test
    public void shouldAdvanceCtWhenWriterSubmitInitiatedAndCompletedTimesWithThreadedImplementation()
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts = assistant.newThreadedQueuedCompletionTimeService( timeSource, errorReporter );

        // Then
        try
        {
            shouldAdvanceCtWhenWriterSubmitInitiatedAndCompletedTimes( cts );
        }
        finally
        {
            cts.shutdown();
        }
    }

    private void shouldAdvanceCtWhenWriterSubmitInitiatedAndCompletedTimes(
            CompletionTimeService cts )
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        CompletionTimeWriter writer1 = cts.newCompletionTimeWriter();
        CompletionTimeWriter writer2 = cts.newCompletionTimeWriter();

        // When
        writer1.submitInitiatedTime( 0L );
        writer1.submitCompletedTime( 0L );
        writer2.submitInitiatedTime( 0L );
        writer2.submitCompletedTime( 0L );

        writer1.submitInitiatedTime( 1L );
        writer2.submitInitiatedTime( 1L );

        // Then
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 0L ) );
        assertThat( cts.completionTimeAsMilli(), is( 0L ) );
    }

    @Test
    public void shouldReturnAllWritersWithSynchronizedImplementation() throws CompletionTimeException
    {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts = assistant.newSynchronizedCompletionTimeService();

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
        CompletionTimeService cts = assistant.newThreadedQueuedCompletionTimeService( timeSource, errorReporter );

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

        CompletionTimeWriter writer1 = cts.newCompletionTimeWriter();

        assertThat( cts.getAllWriters().size(), is( 1 ) );
        assertThat( cts.getAllWriters().contains( writer1 ), is( true ) );

        CompletionTimeWriter writer2 = cts.newCompletionTimeWriter();
        CompletionTimeWriter writer3 = cts.newCompletionTimeWriter();

        assertThat( cts.getAllWriters().size(), is( 3 ) );
        assertThat( cts.getAllWriters().contains( writer1 ), is( true ) );
        assertThat( cts.getAllWriters().contains( writer2 ), is( true ) );
        assertThat( cts.getAllWriters().contains( writer3 ), is( true ) );
    }

    @Test
    public void shouldReturnNullWhenNoITNoCTWithSynchronizedImplementation()
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts = assistant.newSynchronizedCompletionTimeService();

        // Then
        try
        {
            doShouldReturnNullWhenNoITNoCT( cts );
        }
        finally
        {
            cts.shutdown();
        }
    }

    @Test
    public void shouldReturnNullWhenNoITNoCTWithThreadedImplementation()
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts = assistant.newThreadedQueuedCompletionTimeService( timeSource, errorReporter );

        // Then
        try
        {
            doShouldReturnNullWhenNoITNoCT( cts );
        }
        finally
        {
            cts.shutdown();
        }
    }

    // IT = none, CT = none --> null
    private void doShouldReturnNullWhenNoITNoCT( CompletionTimeService cts )
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given

        // When
        // no events have been initiated or completed

        // Then
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( -1L ) );
    }

    @Test
    public void shouldReturnNullWhenSomeITAndNoCTWithSynchronizedImplementation()
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts = assistant.newSynchronizedCompletionTimeService();

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
        CompletionTimeService cts = assistant.newThreadedQueuedCompletionTimeService( timeSource, errorReporter );

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

    // IT = some, CT = none --> null
    private void doShouldReturnNullWhenSomeITAndNoCT( CompletionTimeService cts )
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        CompletionTimeWriter completionTimeWriter = cts.newCompletionTimeWriter();

        // When
        completionTimeWriter.submitInitiatedTime( 1000L );

        // Then
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( -1L ) );
    }

    @Test
    public void shouldReturnNullWhenSomeITAndSomeCTWithSynchronizedImplementation()
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts = assistant.newSynchronizedCompletionTimeService();

        // Then
        try
        {
            doShouldReturnNullWhenSomeITAndSomeCT( cts );
        }
        finally
        {
            cts.shutdown();
        }
    }

    @Test
    public void shouldReturnNullWhenSomeITAndSomeCTWithThreadedImplementation()
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts = assistant.newThreadedQueuedCompletionTimeService( timeSource, errorReporter );

        // Then
        try
        {
            doShouldReturnNullWhenSomeITAndSomeCT( cts );
        }
        finally
        {
            cts.shutdown();
        }
    }

    //  IT = some, CT = some --> null
    private void doShouldReturnNullWhenSomeITAndSomeCT( CompletionTimeService cts )
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        CompletionTimeWriter completionTimeWriter = cts.newCompletionTimeWriter();

        // When
        completionTimeWriter.submitInitiatedTime( 1000L );
        completionTimeWriter.submitCompletedTime( 1000L );

        // Then
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( -1L ) );
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWithSynchronizedImplementation()
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts = assistant.newSynchronizedCompletionTimeService();

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
        CompletionTimeService cts = assistant.newThreadedQueuedCompletionTimeService( timeSource, errorReporter );

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
        CompletionTimeWriter completionTimeWriter = cts.newCompletionTimeWriter();

        // When/Then
        // initiated [1]
        // completed []
        completionTimeWriter.submitInitiatedTime( 1000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( -1L ) );

        // initiated [1]
        // completed [1]
        completionTimeWriter.submitCompletedTime( 1000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( -1L ) );

        // initiated [1,2]
        // completed [1]
        completionTimeWriter.submitInitiatedTime( 2000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 1000L
        ) );

        // initiated [1,2]
        // completed [1,2]
        completionTimeWriter.submitCompletedTime( 2000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 1000L
        ) );

        // initiated [1,2,3]
        // completed [1,2]
        completionTimeWriter.submitInitiatedTime( 3000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 2000L
        ) );

        // initiated [1,2,3,4]
        // completed [1,2]
        completionTimeWriter.submitInitiatedTime( 4000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 2000L
        ) );

        // initiated [1,2,3,4,5]
        // completed [1,2]
        completionTimeWriter.submitInitiatedTime( 5000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 2000L
        ) );

        // initiated [1,2,3,4,5]
        // completed [1,2, , ,5]
        completionTimeWriter.submitCompletedTime( 5000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 2000L
        ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, , , , ,  ]
        completionTimeWriter.submitInitiatedTime( 6000L );
        completionTimeWriter.submitInitiatedTime( 7000L );
        completionTimeWriter.submitInitiatedTime( 8000L );
        completionTimeWriter.submitInitiatedTime( 9000L );
        completionTimeWriter.submitInitiatedTime( 10000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 2000L
        ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, , ,8, ,  ]
        completionTimeWriter.submitCompletedTime( 8000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 2000L
        ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, ,7,8, ,  ]
        completionTimeWriter.submitCompletedTime( 7000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 2000L
        ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, ,7,8,9,  ]
        completionTimeWriter.submitCompletedTime( 9000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 2000L
        ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, ,4,5, ,7,8,9,  ]
        completionTimeWriter.submitCompletedTime( 4000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 2000L
        ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2,3,4,5, ,7,8,9,  ]
        completionTimeWriter.submitCompletedTime( 3000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 5000L
        ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2,3,4,5,6,7,8,9,  ]
        completionTimeWriter.submitCompletedTime( 6000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 9000L
        ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2,3,4,5,6,7,8,9,10]
        completionTimeWriter.submitCompletedTime( 10000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 9000L
        ) );

        // initiated [1,2,3,4,5,6,7,8,9,10,11]
        // completed [1,2,3,4,5,6,7,8,9,10,  ]
        completionTimeWriter.submitInitiatedTime( 11000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ),
                is( 10000L ) );
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWithDuplicateTimesWithSynchronizedImplementation()
            throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException
    {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService cts = assistant.newSynchronizedCompletionTimeService();

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
        CompletionTimeService cts = assistant.newThreadedQueuedCompletionTimeService( timeSource, errorReporter );

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
        CompletionTimeWriter ctWriter = cts.newCompletionTimeWriter();

        // When/Then
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( -1L ) );

        // initiated [1]
        // completed [1]
        ctWriter.submitInitiatedTime( 1000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( -1L ) );

        ctWriter.submitCompletedTime( 1000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( -1L ) );

        // initiated [1]
        // completed [1]
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( -1L ) );

        // initiated [1]
        // completed [1]
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( -1L ) );

        // initiated [1,2]
        // completed [1]
        ctWriter.submitInitiatedTime( 2000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 1000L ) );

        // initiated [1,2,3]
        // completed [1]
        ctWriter.submitInitiatedTime( 3000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 1000L ) );

        // initiated [1,2,3,4]
        // completed [1]
        ctWriter.submitInitiatedTime( 4000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 1000L ) );

        // initiated [1,2,3,4]
        // completed [1, , ,4]
        ctWriter.submitCompletedTime( 4000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 1000L ) );

        // initiated [1,2,3,4]
        // completed [1,2, ,4]
        ctWriter.submitCompletedTime( 2000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 2000L ) );

        // initiated [1,2,3,4,5]
        // completed [1,2, ,4, ]
        ctWriter.submitInitiatedTime( 5000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 2000L ) );

        // initiated [1,2,3,4,5]
        // completed [1,2, ,4, ]
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 2000L ) );

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4, ]
        ctWriter.submitCompletedTime( 3000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 4000L ) );

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4,5]
        ctWriter.submitCompletedTime( 5000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 4000L ) );

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4,5]
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 4000L ) );

        // initiated [1,2,3,4,5,6]
        // completed [1,2,3,4,5]
        ctWriter.submitInitiatedTime( 6000L );
        assertThat( cts.completionTimeAsMilliFuture().get( 1, TimeUnit.SECONDS ), is( 5000L ) );
    }
}
