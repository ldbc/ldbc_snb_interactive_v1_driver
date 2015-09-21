package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.TimeSource;

import java.util.List;
import java.util.Set;

public class CompletionTimeServiceAssistant
{
    public void writeInitiatedAndCompletedTimesToAllWriters( CompletionTimeService completionTimeService,
            long timeAsMilli ) throws CompletionTimeException
    {
        List<LocalCompletionTimeWriter> writers = completionTimeService.getAllWriters();
        for ( LocalCompletionTimeWriter writer : writers )
        {
            writer.submitLocalInitiatedTime( timeAsMilli );
            writer.submitLocalCompletedTime( timeAsMilli );
        }
    }

    public boolean waitForGlobalCompletionTime( TimeSource timeSource,
            long globalCompletionTimeToWaitForAsMilli,
            long timeoutDurationAsMilli,
            CompletionTimeService completionTimeService,
            ConcurrentErrorReporter errorReporter ) throws CompletionTimeException
    {
        long sleepDurationAsMilli = 100;
        long timeoutTimeAsMilli = timeSource.nowAsMilli() + timeoutDurationAsMilli;
        while ( timeSource.nowAsMilli() < timeoutTimeAsMilli )
        {
            long currentGlobalCompletionTimeAsMilli = completionTimeService.globalCompletionTimeAsMilli();
            if ( -1 == currentGlobalCompletionTimeAsMilli )
            { continue; }
            if ( globalCompletionTimeToWaitForAsMilli <= currentGlobalCompletionTimeAsMilli )
            { return true; }
            if ( errorReporter.errorEncountered() )
            {
                throw new CompletionTimeException( "Encountered error while waiting for GCT" );
            }
            Spinner.powerNap( sleepDurationAsMilli );
        }
        return false;
    }

    public SynchronizedCompletionTimeService newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(
            Set<String> peerIds ) throws CompletionTimeException
    {
        return new SynchronizedCompletionTimeService( peerIds );
    }

    public ThreadedQueuedCompletionTimeService newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(
            TimeSource timeSource,
            Set<String> peerIds,
            ConcurrentErrorReporter errorReporter ) throws CompletionTimeException
    {
        return new ThreadedQueuedCompletionTimeService( timeSource, peerIds, errorReporter );
    }
}
