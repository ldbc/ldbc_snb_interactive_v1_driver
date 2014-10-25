package com.ldbc.driver.runtime.coordination;

public class MultiWriterLocalCompletionTimeConcurrentStateManagerWriter implements LocalCompletionTimeWriter {
    private final int id;
    private final MultiWriterLocalCompletionTimeConcurrentStateManager localCompletionTimeStateManager;

    MultiWriterLocalCompletionTimeConcurrentStateManagerWriter(int id,
                                                               MultiWriterLocalCompletionTimeConcurrentStateManager localCompletionTimeStateManager) {
        this.id = id;
        this.localCompletionTimeStateManager = localCompletionTimeStateManager;
    }

    @Override
    public void submitLocalInitiatedTime(long timeAsMilli) throws CompletionTimeException {
        localCompletionTimeStateManager.submitLocalInitiatedTime(id, timeAsMilli);
    }

    @Override
    public void submitLocalCompletedTime(long timeAsMilli) throws CompletionTimeException {
        localCompletionTimeStateManager.submitLocalCompletedTime(id, timeAsMilli);
    }

    int id() {
        return id;
    }

    @Override
    public String toString() {
        return "MultiWriterLocalCompletionTimeConcurrentStateManagerWriter{" +
                "id=" + id +
                '}';
    }
}
