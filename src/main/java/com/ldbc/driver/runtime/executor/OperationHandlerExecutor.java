package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.temporal.Duration;

public interface OperationHandlerExecutor {
    /**
     * @param operationHandler
     * @return
     */
    public void execute(OperationHandler<?> operationHandler) throws OperationHandlerExecutorException;

    /**
     * Returns after executor has completed shutting down
     *
     * @param wait duration to wait for all running operation handlers to complete execution
     * @throws OperationHandlerExecutorException
     */
    public void shutdown(Duration wait) throws OperationHandlerExecutorException;
}
