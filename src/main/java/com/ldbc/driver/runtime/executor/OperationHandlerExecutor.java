package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;

import java.util.concurrent.Future;

public interface OperationHandlerExecutor {
    /**
     * @param operationHandler
     * @return Future\<OperationResult\> for the operationHandler that was submitted for execution
     */
    public Future<OperationResult> execute(OperationHandler<?> operationHandler);

    /**
     * Get next OperationResult returned by a submitted OperationHandler. If
     * none are currently available, return null.
     *
     * @return OperationResult if one available, null otherwise
     */
    public OperationResult poll() throws OperationHandlerExecutorException;

    /**
     * Get next OperationResult returned by a submitted OperationHandler. Blocks
     * to wait for the next OperationResult if any OperationHandler is still
     * running. Returns immediately with null if no OperationHandler is till
     * running.
     *
     * @return OperationResult if any are pending, null otherwise
     */
    public OperationResult take() throws OperationHandlerExecutorException;

    public void shutdown() throws OperationHandlerExecutorException;
}
