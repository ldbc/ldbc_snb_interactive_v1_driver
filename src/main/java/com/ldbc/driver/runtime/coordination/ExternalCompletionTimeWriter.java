package com.ldbc.driver.runtime.coordination;

public interface ExternalCompletionTimeWriter {
    void submitPeerCompletionTime(String peerId, long timeAsMilli) throws CompletionTimeException;
}
