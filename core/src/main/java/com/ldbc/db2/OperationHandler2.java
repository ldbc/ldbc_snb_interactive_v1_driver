package com.ldbc.db2;

public abstract class OperationHandler2<A extends Operation2<?>>
{
    public abstract OperationResult2 executeOperation( A operation );
}
