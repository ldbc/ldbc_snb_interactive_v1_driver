package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.TimeSource;

import java.util.List;

public class CompletionTimeServiceAssistant
{
    public void writeInitiatedAndCompletedTimesToAllWriters(
            CompletionTimeService completionTimeService,
            long timeAsMilli ) throws CompletionTimeException
    {
        List<CompletionTimeWriter> writers = completionTimeService.getAllWriters();
        for ( CompletionTimeWriter writer : writers )
        {
            writer.submitInitiatedTime( timeAsMilli );
            writer.submitCompletedTime( timeAsMilli );
        }
    }

    public boolean waitForCompletionTime(
            TimeSource timeSource,
            long completionTimeToWaitForAsMilli,
            long timeoutDurationAsMilli,
            CompletionTimeService completionTimeService,
            ConcurrentErrorReporter errorReporter ) throws CompletionTimeException
    {
        long sleepDurationAsMilli = 100;
        long timeoutTimeAsMilli = timeSource.nowAsMilli() + timeoutDurationAsMilli;
        while ( timeSource.nowAsMilli() < timeoutTimeAsMilli )
        {
            long currentCompletionTimeAsMilli = completionTimeService.completionTimeAsMilli();
            if ( -1 == currentCompletionTimeAsMilli )
            { continue; }
            if ( completionTimeToWaitForAsMilli <= currentCompletionTimeAsMilli )
            { return true; }
            if ( errorReporter.errorEncountered() )
            {
                throw new CompletionTimeException( "Encountered error while waiting for CT" );
            }
            Spinner.powerNap( sleepDurationAsMilli );
        }
        return false;
    }

    public SynchronizedCompletionTimeService newSynchronizedCompletionTimeService() throws CompletionTimeException
    {
        return new SynchronizedCompletionTimeService();
    }

    public ThreadedQueuedCompletionTimeService newThreadedQueuedCompletionTimeService(
            TimeSource timeSource,
            ConcurrentErrorReporter errorReporter ) throws CompletionTimeException
    {
        return new ThreadedQueuedCompletionTimeService( timeSource, errorReporter );
    }
}
