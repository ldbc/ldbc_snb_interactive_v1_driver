package com.ldbc.db;

public class OperationResult
{
    private final int resultCode;
    private final Object result;

    OperationResult( int resultCode, Object result )
    {
        super();
        this.resultCode = resultCode;
        this.result = result;
    }

    public final int getResultCode()
    {
        return resultCode;
    }

    public final Object getResult()
    {
        return result;
    }
}
