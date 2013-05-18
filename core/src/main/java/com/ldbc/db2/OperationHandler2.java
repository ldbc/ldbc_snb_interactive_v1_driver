package com.ldbc.db2;

public abstract class OperationHandler2<A extends Operation2<?>>
{
    public final OperationResult2 execute( Operation2<?> operation )
    {
        return executeOperation( (A) operation );
    }

    protected abstract OperationResult2 executeOperation( A operation );
}
