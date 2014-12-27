//package com.ldbc.driver.runtime.executor;
//
//import com.ldbc.driver.OperationHandlerRunnableContext;
//
//public interface OperationHandlerExecutor {
//    /**
//     * @param operationHandlerRunnableContext
//     * @return
//     */
//    public void execute(OperationHandlerRunnableContext operationHandlerRunnableContext) throws OperationHandlerExecutorException;
//
//    /**
//     * Returns after executor has completed shutting down
//     *
//     * @param waitAsMilli duration to wait for all running operation handlers to complete execution
//     * @throws com.ldbc.driver.runtime.executor.OperationHandlerExecutorException
//     */
//    public void shutdown(long waitAsMilli) throws OperationHandlerExecutorException;
//
//    public long uncompletedOperationHandlerCount();
//}
