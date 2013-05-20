package com.ldbc.db;

public abstract class Operation<R>
{
    public final OperationResult buildResult( int resultCode, R result )
    {
        return new OperationResult( resultCode, result );
    }
}
