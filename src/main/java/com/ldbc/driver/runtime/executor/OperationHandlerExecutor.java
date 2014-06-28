package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.temporal.Duration;

import java.util.concurrent.Future;

public interface OperationHandlerExecutor {
    /**
     * @param operationHandler
     * @return Future\<OperationResultReport\> for the OperationHandler that was submitted for execution
     */
    public Future<OperationResultReport> execute(OperationHandler<?> operationHandler) throws OperationHandlerExecutorException;

    /**
     * Returns after executor has completed shutting down
     *
     * @param wait duration to wait for all running operation handlers to complete execution
     * @throws OperationHandlerExecutorException
     */
    public void shutdown(Duration wait) throws OperationHandlerExecutorException;
}
