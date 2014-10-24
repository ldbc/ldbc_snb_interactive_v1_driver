package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.OperationHandler;

public interface OperationHandlerExecutor {
    /**
     * @param operationHandler
     * @return
     */
    public void execute(OperationHandler<?> operationHandler) throws OperationHandlerExecutorException;

    /**
     * Returns after executor has completed shutting down
     *
     * @param waitAsMilli duration to wait for all running operation handlers to complete execution
     * @throws OperationHandlerExecutorException
     */
    public void shutdown(long waitAsMilli) throws OperationHandlerExecutorException;

    public long uncompletedOperationHandlerCount();
}
