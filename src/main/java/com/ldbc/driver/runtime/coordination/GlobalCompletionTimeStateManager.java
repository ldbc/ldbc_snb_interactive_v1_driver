package com.ldbc.driver.runtime.coordination;

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
    public void submitPeerCompletionTime(String peerId, long peerCompletionTimeAsMilli) throws CompletionTimeException {
        externalCompletionTimeWriter.submitPeerCompletionTime(peerId, peerCompletionTimeAsMilli);
    }

    @Override
    public void submitLocalInitiatedTime(long timeAsMilli) throws CompletionTimeException {
        localCompletionTimeWriter.submitLocalInitiatedTime(timeAsMilli);
    }

    @Override
    public void submitLocalCompletedTime(long timeAsMilli) throws CompletionTimeException {
        localCompletionTimeWriter.submitLocalCompletedTime(timeAsMilli);
    }

    @Override
    public long globalCompletionTimeAsMilli() throws CompletionTimeException {
        long localCompletionTimeValue = localCompletionTimeReader.localCompletionTimeAsMilli();
        if (-1 == localCompletionTimeValue)
            // Until we know what our local completion time is there is no way of knowing what GCT is
            return -1;

        long externalCompletionTimeValue = externalCompletionTimeReader.externalCompletionTimeAsMilli();
        if (-1 == externalCompletionTimeValue)
            // One or more of our peers have not replied yet -> no way of knowing what GCT is
            return -1;

        // Return min(localCompletionTime,externalCompletionTime)
        return (localCompletionTimeValue < externalCompletionTimeValue)
                ? localCompletionTimeValue
                : externalCompletionTimeValue;
    }
}
