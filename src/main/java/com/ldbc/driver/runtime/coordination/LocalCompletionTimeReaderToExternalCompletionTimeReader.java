package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.temporal.Time;

public class LocalCompletionTimeReaderToExternalCompletionTimeReader implements ExternalCompletionTimeReader {
    private final LocalCompletionTimeReader localCompletionTimeReader;

    LocalCompletionTimeReaderToExternalCompletionTimeReader(LocalCompletionTimeReader localCompletionTimeReader) {
        this.localCompletionTimeReader = localCompletionTimeReader;
    }

    @Override
    public Time externalCompletionTime() throws CompletionTimeException {
        return localCompletionTimeReader.localCompletionTime();
    }
}
