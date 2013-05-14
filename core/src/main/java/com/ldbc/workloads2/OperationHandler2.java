package com.ldbc.workloads2;

public abstract class OperationHandler2<A extends OperationArgs2>
{
    public abstract void executeOperation( A operationArgs );
}
