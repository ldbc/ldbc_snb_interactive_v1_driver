package com.ldbc.driver;

public abstract class Operation<R>
{
    private long scheduledStartTime = unassignedScheduledStartTime();

    public final void setScheduledStartTime( long scheduledStartTime )
    {
        this.scheduledStartTime = scheduledStartTime;
    }

    public static long unassignedScheduledStartTime()
    {
        return -1;
    }

    public final long getScheduledStartTime()
    {
        return scheduledStartTime;
    }

    public final OperationResult buildResult( int resultCode, R result )
    {
        return new OperationResult( resultCode, result );
    }

    @Override
    public String toString()
    {
        return String.format( "Operation [type=%s, scheduledStartTime=%s]", getClass().getName(), scheduledStartTime );
    }
}
