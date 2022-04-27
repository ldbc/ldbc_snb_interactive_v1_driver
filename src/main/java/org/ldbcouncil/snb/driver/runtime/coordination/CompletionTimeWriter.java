package org.ldbcouncil.snb.driver.runtime.coordination;

public interface CompletionTimeWriter
{
    void submitInitiatedTime( long timeAsMilli ) throws CompletionTimeException;

    void submitCompletedTime( long timeAsMilli ) throws CompletionTimeException;
}
