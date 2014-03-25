package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;

import java.util.concurrent.Future;

public interface OperationHandlerExecutor {
    /**
     * @param operationHandler
     * @return Future\<OperationResult\> for the OperationHandler that was submitted for execution
     */
    public Future<OperationResult> execute(OperationHandler<?> operationHandler) throws OperationHandlerExecutorException;

    /**
     * Returns after executor has completed shutting down
     *
     * @throws OperationHandlerExecutorException
     */
    public void shutdown() throws OperationHandlerExecutorException;
}
