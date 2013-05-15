package com.ldbc.db2;

public class OperationResult2
{
    private final int resultCode;
    private final Object result;

    OperationResult2( int resultCode, Object result )
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
