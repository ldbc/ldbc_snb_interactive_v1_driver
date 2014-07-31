package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.temporal.Time;

public interface ExternalCompletionTimeWriter {
    void submitPeerCompletionTime(String peerId, Time time) throws CompletionTimeException;
}
