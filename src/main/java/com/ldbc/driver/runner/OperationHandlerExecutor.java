package com.ldbc.driver.runner;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;

public interface OperationHandlerExecutor {
    public void execute(OperationHandler<?> operationHandler);

    /**
     * Get next OperationResult returned by a submitted OperationHandler. If
     * none are currently available, return null.
     *
     * @return OperationResult if one available, null otherwise
     */
    public OperationResult nextOperationResultNonBlocking() throws OperationHandlerExecutorException;

    /**
     * Get next OperationResult returned by a submitted OperationHandler. Blocks
     * to wait for the next OperationResult if any OperationHandler is still
     * running. Returns immediately with null if no OperationHandler is till
     * running.
     *
     * @return OperationResult if any are pending, null otherwise
     */
    public OperationResult nextOperationResultBlocking() throws OperationHandlerExecutorException;

    public void shutdown() throws OperationHandlerExecutorException;
}
