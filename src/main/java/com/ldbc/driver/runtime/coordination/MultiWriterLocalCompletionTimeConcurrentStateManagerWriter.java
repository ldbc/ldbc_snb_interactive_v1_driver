package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.temporal.Time;

public class MultiWriterLocalCompletionTimeConcurrentStateManagerWriter implements LocalCompletionTimeWriter {
    private final int id;
    private final MultiWriterLocalCompletionTimeConcurrentStateManager localCompletionTimeStateManager;

    MultiWriterLocalCompletionTimeConcurrentStateManagerWriter(int id,
                                                               MultiWriterLocalCompletionTimeConcurrentStateManager localCompletionTimeStateManager) {
        this.id = id;
        this.localCompletionTimeStateManager = localCompletionTimeStateManager;
    }

    @Override
    public void submitLocalInitiatedTime(Time scheduledStartTime) throws CompletionTimeException {
        localCompletionTimeStateManager.submitLocalInitiatedTime(id, scheduledStartTime);
    }

    @Override
    public void submitLocalCompletedTime(Time scheduledStartTime) throws CompletionTimeException {
        localCompletionTimeStateManager.submitLocalCompletedTime(id, scheduledStartTime);
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
