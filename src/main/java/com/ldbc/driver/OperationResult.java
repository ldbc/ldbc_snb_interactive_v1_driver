package com.ldbc.driver;

import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

public class OperationResult
{
    private final int resultCode;
    private final Object result;

    private Time scheduledStartTime = null;
    private Time actualStartTime = null;
    private Duration runDuration = null;
    private String operationType = null;

    // TODO public - just for testing at present?
    public OperationResult( int resultCode, Object result )
    {
        super();
        this.resultCode = resultCode;
        this.result = result;
    }

    public int resultCode()
    {
        return resultCode;
    }

    public Object result()
    {
        return result;
    }

    public Time scheduledStartTime()
    {
        return scheduledStartTime;
    }

    // TODO public - just for testing at present?
    public void setScheduledStartTime( Time scheduledStartTime )
    {
        this.scheduledStartTime = scheduledStartTime;
    }

    public Time actualStartTime()
    {
        return actualStartTime;
    }

    // TODO public - just for testing at present?
    public void setActualStartTime( Time actualStartTime )
    {
        this.actualStartTime = actualStartTime;
    }

    public Duration runDuration()
    {
        return runDuration;
    }

    // TODO public - just for testing at present?
    public void setRunDuration( Duration runDuration )
    {
        this.runDuration = runDuration;
    }

    public String operationType()
    {
        return operationType;
    }

    // TODO public - just for testing at present?
    public void setOperationType( String operationType )
    {
        this.operationType = operationType;
    }

    @Override
    public String toString()
    {
        return "OperationResult [resultCode=" + resultCode + ", result=" + result + ", scheduledStartTime="
               + scheduledStartTime + ", actualStartTime=" + actualStartTime + ", runDuration=" + runDuration
               + ", operationType=" + operationType + "]";
    }
}
