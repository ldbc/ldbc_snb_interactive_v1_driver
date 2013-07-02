package com.ldbc.driver;

import com.ldbc.driver.util.temporal.Duration;
import com.ldbc.driver.util.temporal.Time;

public class OperationResult
{
    private final int resultCode;
    private final Object result;

    private Time scheduledStartTime = Time.fromNano( -1 );
    private Time actualStartTime = Time.fromNano( -1 );
    private Duration runTime = Duration.fromNano( -1 );
    private String operationType = null;

    // TODO public - just for testing at present?
    public OperationResult( int resultCode, Object result )
    {
        super();
        this.resultCode = resultCode;
        this.result = result;
    }

    public int getResultCode()
    {
        return resultCode;
    }

    public Object getResult()
    {
        return result;
    }

    public Time getScheduledStartTime()
    {
        return scheduledStartTime;
    }

    // TODO public - just for testing at present?
    public void setScheduledStartTime( Time scheduledStartTime )
    {
        this.scheduledStartTime = scheduledStartTime;
    }

    public Time getActualStartTime()
    {
        return actualStartTime;
    }

    // TODO public - just for testing at present?
    public void setActualStartTime( Time actualStartTime )
    {
        this.actualStartTime = actualStartTime;
    }

    public Duration getRunTime()
    {
        return runTime;
    }

    // TODO public - just for testing at present?
    public void setRunTime( Duration runTime )
    {
        this.runTime = runTime;
    }

    public String getOperationType()
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
               + scheduledStartTime + ", actualStartTime=" + actualStartTime + ", runTime=" + runTime
               + ", operationType=" + operationType + "]";
    }
}
