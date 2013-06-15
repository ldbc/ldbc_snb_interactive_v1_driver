package com.ldbc.driver;

public abstract class Operation<R>
{
    public final static long UNASSIGNED_SCHEDULED_START_TIME = -1;

    private long scheduledStartTimeNanoSeconds = UNASSIGNED_SCHEDULED_START_TIME;

    public final void setScheduledStartTimeNanoSeconds( long scheduledStartTimeNanoSeconds )
    {
        this.scheduledStartTimeNanoSeconds = scheduledStartTimeNanoSeconds;
    }

    public final Long getScheduledStartTimeNanoSeconds()
    {
        return scheduledStartTimeNanoSeconds;
    }

    public final OperationResult buildResult( int resultCode, R result )
    {
        return new OperationResult( resultCode, result );
    }

    @Override
    public String toString()
    {
        return String.format( "Operation [type=%s, scheduledStartTime=%s]", getClass().getName(),
                scheduledStartTimeNanoSeconds );
    }
}
