package com.ldbc.driver;

public interface OperationHandlerRunnerFactory
{
    OperationHandlerRunnableContext newOperationHandlerRunner() throws OperationException;

    void shutdown() throws OperationException;
}
