package com.ldbc.driver.runtime.coordination;

public class GlobalCompletionTimeStateManager implements
        LocalCompletionTimeWriter,
        GlobalCompletionTimeReader
{

    private final LocalCompletionTimeReader localCompletionTimeReader;
    private final LocalCompletionTimeWriter localCompletionTimeWriter;

    GlobalCompletionTimeStateManager(
            LocalCompletionTimeReader localCompletionTimeReader,
            LocalCompletionTimeWriter localCompletionTimeWriter )
    {
        this.localCompletionTimeReader = localCompletionTimeReader;
        this.localCompletionTimeWriter = localCompletionTimeWriter;
    }


    @Override
    public void submitLocalInitiatedTime( long timeAsMilli ) throws CompletionTimeException
    {
        localCompletionTimeWriter.submitLocalInitiatedTime( timeAsMilli );
    }

    @Override
    public void submitLocalCompletedTime( long timeAsMilli ) throws CompletionTimeException
    {
        localCompletionTimeWriter.submitLocalCompletedTime( timeAsMilli );
    }

    @Override
    public long globalCompletionTimeAsMilli() throws CompletionTimeException
    {
        long localCompletionTimeValue = localCompletionTimeReader.localCompletionTimeAsMilli();
        return (-1 == localCompletionTimeValue) ? -1 : localCompletionTimeValue;
    }
}
