package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.Operation;

public interface OperationExecutor {
    /**
     * @param operation
     * @return
     */
    public void execute(Operation operation) throws OperationExecutorException;

    /**
     * Returns after executor has completed shutting down
     *
     * @param waitAsMilli duration to wait for all running operation handlers to complete execution
     * @throws OperationExecutorException
     */
    public void shutdown(long waitAsMilli) throws OperationExecutorException;

    public long uncompletedOperationHandlerCount();
}
