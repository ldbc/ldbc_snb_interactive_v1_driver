package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.temporal.Time;

public class GlobalCompletionTimeStateManager implements
        LocalCompletionTimeWriter,
        ExternalCompletionTimeWriter,
        GlobalCompletionTimeReader {

    private final LocalCompletionTimeReader localCompletionTimeReader;
    private final LocalCompletionTimeWriter localCompletionTimeWriter;
    private final ExternalCompletionTimeReader externalCompletionTimeReader;
    private final ExternalCompletionTimeWriter externalCompletionTimeWriter;

    GlobalCompletionTimeStateManager(
            LocalCompletionTimeReader localCompletionTimeReader,
            LocalCompletionTimeWriter localCompletionTimeWriter,
            ExternalCompletionTimeReader externalCompletionTimeReader,
            ExternalCompletionTimeWriter externalCompletionTimeWriter) {
        this.localCompletionTimeReader = localCompletionTimeReader;
        this.localCompletionTimeWriter = localCompletionTimeWriter;
        this.externalCompletionTimeReader = externalCompletionTimeReader;
        this.externalCompletionTimeWriter = externalCompletionTimeWriter;
    }


    @Override
    public void submitPeerCompletionTime(String peerId, Time peerCompletionTime) throws CompletionTimeException {
        externalCompletionTimeWriter.submitPeerCompletionTime(peerId, peerCompletionTime);
    }

    @Override
    public void submitLocalInitiatedTime(Time timeAsMilli) throws CompletionTimeException {
        localCompletionTimeWriter.submitLocalInitiatedTime(timeAsMilli);
    }

    @Override
    public void submitLocalCompletedTime(Time timeAsMilli) throws CompletionTimeException {
        localCompletionTimeWriter.submitLocalCompletedTime(timeAsMilli);
    }

    @Override
    public Time globalCompletionTimeAsMilli() throws CompletionTimeException {
        Time localCompletionTimeValue = localCompletionTimeReader.localCompletionTime();
        if (null == localCompletionTimeValue)
            // Until we know what our local completion time is there is no way of knowing what GCT is
            return null;

        Time externalCompletionTimeValue = externalCompletionTimeReader.externalCompletionTime();
        if (null == externalCompletionTimeValue)
            // One or more of our peers have not replied yet -> no way of knowing what GCT is
            return null;

        // Return min(localCompletionTime,externalCompletionTime)
        return (localCompletionTimeValue.lt(externalCompletionTimeValue))
                ? localCompletionTimeValue
                : externalCompletionTimeValue;
    }
}
