package com.ldbc.driver;

import com.ldbc.driver.temporal.TemporalUtil;
import stormpot.*;

import java.util.concurrent.TimeUnit;

public class PoolingOperationHandlerFactory implements OperationHandlerFactory {
    private static final Timeout POOL_CLAIM_TIMEOUT = new Timeout(100, TimeUnit.MILLISECONDS);
    private static final Timeout POOL_CLAIM_AFTER_RESIZE_TIMEOUT = new Timeout(1000, TimeUnit.MILLISECONDS);
    private final LifecycledResizablePool<OperationHandler<?>> operationHandlerPool;
    private final OperationHandlerFactory innerOperationHandlerFactory;

    public PoolingOperationHandlerFactory(OperationHandlerFactory operationHandlerFactory) {
        this.innerOperationHandlerFactory = operationHandlerFactory;
        OperationHandlerAllocator operationHandlerAllocator = new OperationHandlerAllocator(innerOperationHandlerFactory);
        Config<OperationHandler<?>> operationHandlerPoolConfig = new Config<>();
        operationHandlerPoolConfig.setAllocator(operationHandlerAllocator);
        this.operationHandlerPool = new BlazePool<>(operationHandlerPoolConfig);
        this.operationHandlerPool.setTargetSize(64);
    }

    @Override
    public OperationHandler<?> newOperationHandler() throws OperationException {
        try {
            OperationHandler<?> operationHandler = operationHandlerPool.claim(POOL_CLAIM_TIMEOUT);
            while (null == operationHandler) {
                operationHandlerPool.setTargetSize(operationHandlerPool.getTargetSize() * 2);
                operationHandler = operationHandlerPool.claim(POOL_CLAIM_AFTER_RESIZE_TIMEOUT);
            }
            return operationHandler;
        } catch (InterruptedException e) {
            throw new OperationException("Error encountered while attempting to allocate handler from pool", e);
        }
    }

    @Override
    public void shutdown() throws OperationException {
        TemporalUtil temporalUtil = new TemporalUtil();
        Timeout timeout = new Timeout(5, TimeUnit.SECONDS);
        innerOperationHandlerFactory.shutdown();
        Completion completion = operationHandlerPool.shutdown();
        try {
            boolean isSuccessfulShutdown = completion.await(timeout);
            if (false == isSuccessfulShutdown)
                throw new OperationException(
                        String.format(
                                "Operation handler pool did not shutdown before timeout (%s)",
                                temporalUtil.milliDurationToString(temporalUtil.convert(timeout.getTimeout(), timeout.getUnit(), TimeUnit.MILLISECONDS))
                        )
                );
        } catch (InterruptedException e) {
            throw new OperationException("Error encountered while shutting down operation handler pool", e);
        }
    }

    @Override
    public String toString() {
        return PoolingOperationHandlerFactory.class.getSimpleName() + "{" + innerOperationHandlerFactory.toString() + "}";
    }

    private static class OperationHandlerAllocator implements Allocator<OperationHandler<?>> {
        private final OperationHandlerFactory operationHandlerFactory;

        public OperationHandlerAllocator(OperationHandlerFactory operationHandlerFactory) {
            this.operationHandlerFactory = operationHandlerFactory;
        }

        @Override
        public OperationHandler<?> allocate(Slot slot) throws Exception {
            OperationHandler<?> operationHandler = operationHandlerFactory.newOperationHandler();
            operationHandler.setSlot(slot);
            return operationHandler;
        }

        @Override
        public void deallocate(OperationHandler operationHandler) throws Exception {
            // I think nothing needs to be done here
        }
    }
}
