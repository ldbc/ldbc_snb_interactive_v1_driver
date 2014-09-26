package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.temporal.Duration;

import java.util.concurrent.atomic.AtomicLong;

public class SameThreadOperationHandlerExecutor implements OperationHandlerExecutor {
    private final AtomicLong uncompletedHandlers = new AtomicLong(0);

    @Override
    public final void execute(OperationHandler<?> operationHandler) throws OperationHandlerExecutorException {
        uncompletedHandlers.incrementAndGet();
        operationHandler.run();
        operationHandler.onComplete();
        operationHandler.cleanup();
        uncompletedHandlers.decrementAndGet();
    }

    @Override
    synchronized public final void shutdown(Duration wait) throws OperationHandlerExecutorException {
    }

    @Override
    public long uncompletedOperationHandlerCount() throws OperationHandlerExecutorException {
        return uncompletedHandlers.get();
    }
}
