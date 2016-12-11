package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class CompletionTimeServiceAssistantTest
{
    @Test
    public void shouldWaitUntilCtAdvancesAndReturnCorrectSuccessValue() throws CompletionTimeException
    {
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        CompletionTimeService completionTimeService = assistant.newSynchronizedCompletionTimeService();
        try
        {
            // initial CT should be null
            assertThat( completionTimeService.completionTimeAsMilli(), is( -1L ) );

            // there are no writers, CT will never advance
            boolean ctAdvancedSuccessfully = assistant.waitForCompletionTime(
                    timeSource,
                    0,
                    1000,
                    completionTimeService,
                    errorReporter );
            assertThat( ctAdvancedSuccessfully, is( false ) );
            assertThat( completionTimeService.completionTimeAsMilli(), is( -1L ) );

            CompletionTimeWriter writer1 = completionTimeService.newCompletionTimeWriter();
            CompletionTimeWriter writer2 = completionTimeService.newCompletionTimeWriter();

            // no initiated/completed times have been submitted, CT will never advance
            ctAdvancedSuccessfully = assistant.waitForCompletionTime(
                    timeSource,
                    0,
                    1000,
                    completionTimeService,
                    errorReporter );
            assertThat( ctAdvancedSuccessfully, is( false ) );
            assertThat( completionTimeService.completionTimeAsMilli(), is( -1L ) );

            assistant.writeInitiatedAndCompletedTimesToAllWriters( completionTimeService, 0 );

            // CT can not be known at this stage, because more 0 times/values may arrive later
            // IT[ ] CT[0] --> CT = ?
            ctAdvancedSuccessfully = assistant.waitForCompletionTime(
                    timeSource,
                    0,
                    1000,
                    completionTimeService,
                    errorReporter );
            assertThat( ctAdvancedSuccessfully, is( false ) );
            assertThat( completionTimeService.completionTimeAsMilli(), is( -1L ) );

            assistant.writeInitiatedAndCompletedTimesToAllWriters( completionTimeService, 1 );

            // CT should now be 0, because no more 0 values/times can come after 1 values/times have been written to
            // all writers
            // IT[ , ] CT[0,1] --> CT = 0
            ctAdvancedSuccessfully = assistant.waitForCompletionTime(
                    timeSource,
                    0,
                    1000,
                    completionTimeService,
                    errorReporter );

            assertThat( ctAdvancedSuccessfully, is( true ) );
            assertThat( completionTimeService.completionTimeAsMilli(), is( 0L ) );
        }
        finally
        {
            completionTimeService.shutdown();
        }
    }
}
