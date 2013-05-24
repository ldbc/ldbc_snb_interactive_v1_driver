package com.ldbc.driver;

public class OperationResult
{
    private final int resultCode;
    private final Object result;

    private long scheduledStartTime = -1;
    private long actualStartTime = -1;
    private long runTime = -1;
    private String operationType = null;

    OperationResult( int resultCode, Object result )
    {
        super();
        this.resultCode = resultCode;
        this.result = result;
    }

    int getResultCode()
    {
        return resultCode;
    }

    Object getResult()
    {
        return result;
    }

    long getScheduledStartTime()
    {
        return scheduledStartTime;
    }

    void setScheduledStartTime( long scheduledStartTime )
    {
        this.scheduledStartTime = scheduledStartTime;
    }

    long getActualStartTime()
    {
        return actualStartTime;
    }

    void setActualStartTime( long actualStartTime )
    {
        this.actualStartTime = actualStartTime;
    }

    long getRunTime()
    {
        return runTime;
    }

    void setRunTime( long runTime )
    {
        this.runTime = runTime;
    }

    String getOperationType()
    {
        return operationType;
    }

    void setOperationType( String operationType )
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
