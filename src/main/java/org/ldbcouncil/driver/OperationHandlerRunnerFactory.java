package org.ldbcouncil.driver;

public interface OperationHandlerRunnerFactory
{
    OperationHandlerRunnableContext newOperationHandlerRunner() throws OperationException;

    void shutdown() throws OperationException;
}
