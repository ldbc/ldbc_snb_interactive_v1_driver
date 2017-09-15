package com.ldbc.driver.runtime.coordination;

public class MultiWriterCompletionTimeStateManagerWriter implements CompletionTimeWriter
{
    private final int id;
    private final MultiWriterCompletionTimeStateManager completionTimeStateManager;

    MultiWriterCompletionTimeStateManagerWriter(
            int id,
            MultiWriterCompletionTimeStateManager completionTimeStateManager )
    {
        this.id = id;
        this.completionTimeStateManager = completionTimeStateManager;
    }

    @Override
    public void submitInitiatedTime( long timeAsMilli ) throws CompletionTimeException
    {
        completionTimeStateManager.submitInitiatedTime( id, timeAsMilli );
    }

    @Override
    public void submitCompletedTime( long timeAsMilli ) throws CompletionTimeException
    {
        completionTimeStateManager.submitCompletedTime( id, timeAsMilli );
    }

    int id()
    {
        return id;
    }

    @Override
    public String toString()
    {
        return "MultiWriterCompletionTimeStateManagerWriter{" + "id=" + id + '}';
    }
}
