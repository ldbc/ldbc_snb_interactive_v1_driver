package com.ldbc.driver.runtime.coordination;

public class LocalCompletionTimeReaderToExternalCompletionTimeReader implements ExternalCompletionTimeReader {
    private final LocalCompletionTimeReader localCompletionTimeReader;

    LocalCompletionTimeReaderToExternalCompletionTimeReader(LocalCompletionTimeReader localCompletionTimeReader) {
        this.localCompletionTimeReader = localCompletionTimeReader;
    }

    @Override
    public long externalCompletionTimeAsMilli() throws CompletionTimeException {
        return localCompletionTimeReader.localCompletionTimeAsMilli();
    }
}
