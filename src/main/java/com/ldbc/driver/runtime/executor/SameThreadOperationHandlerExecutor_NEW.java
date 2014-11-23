package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.*;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;

import java.util.concurrent.atomic.AtomicLong;

public class SameThreadOperationHandlerExecutor_NEW implements OperationExecutor_NEW {
    private final AtomicLong uncompletedHandlers = new AtomicLong(0);
    private final Db db;

    public SameThreadOperationHandlerExecutor_NEW(Db db) {
        this.db = db;
    }

    @Override
    public final void execute(Operation operation) throws OperationHandlerExecutorException {
        OperationHandlerRunnableContext operationHandlerRunnableContext;
        try {
            operationHandlerRunnableContext = db.getOperationHandlerRunnableContext(operation);
        } catch (DbException e) {
            throw new OperationHandlerExecutorException(
                    String.format("Error retrieving handler\nOperation: %s\n%s",
                            operation,
                            ConcurrentErrorReporter.stackTraceToString(e)),
                    e
            );
        }




        uncompletedHandlers.incrementAndGet();
        operation.run();
        operation.cleanup();
        uncompletedHandlers.decrementAndGet();
    }

    @Override
    synchronized public final void shutdown(long waitAsMilli) throws OperationHandlerExecutorException {
    }

    @Override
    public long uncompletedOperationHandlerCount() {
        return uncompletedHandlers.get();
    }
}
