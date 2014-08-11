package com.ldbc.driver;

public interface OperationHandlerFactory {
    OperationHandler<?> newOperationHandler() throws OperationException;

    void shutdown() throws OperationException;
}
