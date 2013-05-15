package com.ldbc.db2;

public abstract class Operation2<R>
{
    public final OperationResult2 buildResult( int resultCode, R result )
    {
        return new OperationResult2( resultCode, result );
    }
}
