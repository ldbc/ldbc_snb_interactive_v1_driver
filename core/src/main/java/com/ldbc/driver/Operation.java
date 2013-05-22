package com.ldbc.driver;

public abstract class Operation<R>
{
    private long scheduledStartTime = -1;

    public final void setScheduledStartTime( long scheduledStartTime )
    {
        this.scheduledStartTime = scheduledStartTime;
    }

    public final long getScheduledStartTime()
    {
        return scheduledStartTime;
    }

    public final OperationResult buildResult( int resultCode, R result )
    {
        return new OperationResult( resultCode, result );
    }
}
