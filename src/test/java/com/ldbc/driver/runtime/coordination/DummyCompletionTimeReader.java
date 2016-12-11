package com.ldbc.driver.runtime.coordination;

public class DummyCompletionTimeReader implements CompletionTimeReader
{
    long completionTimeAsMilli = -1;

    public void setCompletionTimeAsMilli( long completionTimeAsMilli )
    {
        this.completionTimeAsMilli = completionTimeAsMilli;
    }

    @Override
    // TODO remove from interface
    public long lastKnownLowestInitiatedTimeAsMilli() throws CompletionTimeException
    {
        throw new UnsupportedOperationException( "Unsupported method" );
    }

    @Override
    public long completionTimeAsMilli() throws CompletionTimeException
    {
        return completionTimeAsMilli;
    }
}
