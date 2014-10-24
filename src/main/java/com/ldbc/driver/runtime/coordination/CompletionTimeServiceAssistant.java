package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.TimeSource;

import java.util.List;
import java.util.Set;

public class CompletionTimeServiceAssistant {
    public void writeInitiatedAndCompletedTimesToAllWriters(ConcurrentCompletionTimeService completionTimeService, long timeAsMilli) throws CompletionTimeException {
        List<LocalCompletionTimeWriter> writers = completionTimeService.getAllWriters();
        for (LocalCompletionTimeWriter writer : writers) {
            writer.submitLocalInitiatedTime(timeAsMilli);
            writer.submitLocalCompletedTime(timeAsMilli);
        }
    }

    public boolean waitForGlobalCompletionTime(TimeSource timeSource,
                                               long globalCompletionTimeToWaitForAsMilli,
                                               long timeoutDurationAsMilli,
                                               ConcurrentCompletionTimeService completionTimeService,
                                               ConcurrentErrorReporter errorReporter) throws CompletionTimeException {
        long sleepDurationAsMilli = 100;
        long timeoutTimeAsMilli = timeSource.nowAsMilli() + timeoutDurationAsMilli;
        while (timeSource.nowAsMilli() < timeoutTimeAsMilli) {
            long currentGlobalCompletionTimeAsMilli = completionTimeService.globalCompletionTimeAsMilli();
            if (-1 == currentGlobalCompletionTimeAsMilli)
                continue;
            if (globalCompletionTimeToWaitForAsMilli <= currentGlobalCompletionTimeAsMilli)
                return true;
            if (errorReporter.errorEncountered())
                throw new CompletionTimeException(String.format("Encountered error while waiting for GCT\n%s", errorReporter.toString()));
            Spinner.powerNap(sleepDurationAsMilli);
        }
        return false;
    }

    public SynchronizedConcurrentCompletionTimeService newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(Set<String> peerIds) throws CompletionTimeException {
        return new SynchronizedConcurrentCompletionTimeService(peerIds);
    }

    public ThreadedQueuedConcurrentCompletionTimeService newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(TimeSource timeSource,
                                                                                                                     Set<String> peerIds,
                                                                                                                     ConcurrentErrorReporter errorReporter) throws CompletionTimeException {
        return new ThreadedQueuedConcurrentCompletionTimeService(timeSource, peerIds, errorReporter);
    }
}
