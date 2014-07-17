package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.Time;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

// TODO test
public class CompletionTimeServiceHelper {
    /**
     * Provided completion time service must be freshly created, i.e., have never had any times submitted to it
     *
     * @param completionTimeService
     * @param peerIds
     * @param errorReporter
     * @param initialTime
     * @return
     * @throws CompletionTimeException
     */
    public static ConcurrentCompletionTimeService initializeCompletionTimeService(ConcurrentCompletionTimeService completionTimeService,
                                                                                  Set<String> peerIds,
                                                                                  ConcurrentErrorReporter errorReporter,
                                                                                  Time initialTime) throws CompletionTimeException {
        completionTimeService.submitInitiatedTime(initialTime);
        completionTimeService.submitCompletedTime(initialTime);
        for (String peerId : peerIds) {
            completionTimeService.submitExternalCompletionTime(peerId, initialTime);
        }
        // Wait for workloadStartTime to be applied
        Future<Time> globalCompletionTimeFuture = completionTimeService.globalCompletionTimeFuture();
        while (false == globalCompletionTimeFuture.isDone()) {
            if (errorReporter.errorEncountered())
                throw new CompletionTimeException(String.format("Encountered error while waiting for GCT to initialize. Driver terminating.\n%s", errorReporter.toString()));
        }
        try {
            if (false == globalCompletionTimeFuture.get().equals(initialTime)) {
                throw new CompletionTimeException("Completion time future failed to return expected value");
            }
        } catch (InterruptedException e) {
            throw new CompletionTimeException("Error while waiting for GCT to be applied", e);
        } catch (ExecutionException e) {
            throw new CompletionTimeException("Error while waiting for GCT to be applied", e);
        }
        return completionTimeService;
    }
}
