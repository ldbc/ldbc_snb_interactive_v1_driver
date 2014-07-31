package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;

import java.util.List;
import java.util.Set;

public class CompletionTimeServiceAssistant {
    public void writeInitiatedAndCompletedTimesToAllWriters(ConcurrentCompletionTimeService completionTimeService, Time time) throws CompletionTimeException {
        List<LocalCompletionTimeWriter> writers = completionTimeService.getAllWriters();
        for (LocalCompletionTimeWriter writer : writers) {
            writer.submitLocalInitiatedTime(time);
            writer.submitLocalCompletedTime(time);
        }
    }

    public boolean waitForGlobalCompletionTime(TimeSource timeSource,
                                               Time globalCompletionTimeToWaitFor,
                                               Duration timeoutDuration,
                                               ConcurrentCompletionTimeService completionTimeService,
                                               ConcurrentErrorReporter errorReporter) throws CompletionTimeException {
        long sleepDurationAsMilli = Duration.fromMilli(100).asMilli();
        long timeoutTimeAsMilli = timeSource.now().plus(timeoutDuration).asMilli();
        while (timeSource.nowAsMilli() < timeoutTimeAsMilli) {
            Time currentGlobalCompletionTime = completionTimeService.globalCompletionTime();
            if (null == currentGlobalCompletionTime)
                continue;
            if (globalCompletionTimeToWaitFor.lte(currentGlobalCompletionTime))
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
