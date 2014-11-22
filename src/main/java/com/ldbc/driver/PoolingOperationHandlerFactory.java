package com.ldbc.driver;

import com.ldbc.driver.temporal.TemporalUtil;
import stormpot.*;

import java.util.concurrent.TimeUnit;

public class PoolingOperationHandlerFactory implements OperationHandlerFactory {
    private static final int INITIAL_POOL_SIZE = 512;
    private static final int MAX_POOL_SIZE = (int) Math.round(Math.pow(2, 15)); // ~32,000
    private static final Timeout POOL_CLAIM_TIMEOUT = new Timeout(100, TimeUnit.MILLISECONDS);
    private static final Timeout POOL_CLAIM_AFTER_RESIZE_TIMEOUT = new Timeout(1000, TimeUnit.MILLISECONDS);
    private static final Timeout POOL_SHUTDOWN_TIMEOUT = new Timeout(10, TimeUnit.SECONDS);
    private final BlazePool<OperationHandler<?>> operationHandlerPool;
    private final OperationHandlerFactory innerOperationHandlerFactory;
    int highestSetPoolSize = 0;

    public PoolingOperationHandlerFactory(OperationHandlerFactory operationHandlerFactory) {
        this.innerOperationHandlerFactory = operationHandlerFactory;
        OperationHandlerAllocator operationHandlerAllocator = new OperationHandlerAllocator(innerOperationHandlerFactory);
        Config<OperationHandler<?>> operationHandlerPoolConfig = new Config<>();
        operationHandlerPoolConfig.setAllocator(operationHandlerAllocator);
        operationHandlerPoolConfig.setBackgroundExpirationEnabled(false);
        operationHandlerPoolConfig.setPreciseLeakDetectionEnabled(true);
        this.operationHandlerPool = new BlazePool<>(operationHandlerPoolConfig);
        this.operationHandlerPool.setTargetSize(INITIAL_POOL_SIZE);
        this.highestSetPoolSize = INITIAL_POOL_SIZE;
    }

    @Override
    public OperationHandler<?> newOperationHandler() throws OperationException {
        try {
            OperationHandler<?> operationHandler = operationHandlerPool.claim(POOL_CLAIM_TIMEOUT);
            while (null == operationHandler) {
                int currentPoolSize = operationHandlerPool.getTargetSize();
                if (currentPoolSize < MAX_POOL_SIZE) {
                    operationHandlerPool.setTargetSize(currentPoolSize * 2);
                    highestSetPoolSize = currentPoolSize * 2;
                }
                operationHandler = operationHandlerPool.claim(POOL_CLAIM_AFTER_RESIZE_TIMEOUT);
            }
            return operationHandler;
        } catch (Exception e) {
            int currentPoolSize = operationHandlerPool.getTargetSize();
            throw new OperationException(
                    String.format("Error encountered while attempting to allocate handler from pool\n"
                                    + "Max pool size: %s\n"
                                    + "Highest set pool size: %s\n"
                                    + "Current pool size: %s",
                            MAX_POOL_SIZE,
                            highestSetPoolSize,
                            currentPoolSize),
                    e
            );
        }
    }

    @Override
    public void shutdown() throws OperationException {
        TemporalUtil temporalUtil = new TemporalUtil();
        innerOperationHandlerFactory.shutdown();
        Completion completion = operationHandlerPool.shutdown();
        try {
            boolean isSuccessfulShutdown = completion.await(POOL_SHUTDOWN_TIMEOUT);
            if (false == isSuccessfulShutdown)
                throw new OperationException(
                        String.format(
                                "Operation handler pool did not shutdown before timeout: %s\n"
                                        + "Pool Target Size: %s\n"
                                        + "Pool Allocation Count: %s\n"
                                        + "Pool Failed Allocation Count: %s\n"
                                        + "Pool Leaked Objects Count: %s\n",
                                temporalUtil.milliDurationToString(temporalUtil.convert(POOL_SHUTDOWN_TIMEOUT.getTimeout(), POOL_SHUTDOWN_TIMEOUT.getUnit(), TimeUnit.MILLISECONDS)),
                                operationHandlerPool.getTargetSize(),
                                operationHandlerPool.getAllocationCount(),
                                operationHandlerPool.getFailedAllocationCount(),
                                operationHandlerPool.getLeakedObjectsCount()
                                // TODO percentile stats require MetricsRecorder implementation to work
                                // TODO http://chrisvest.github.io/stormpot/site/apidocs/stormpot/MetricsRecorder.html
//                                operationHandlerPool.getAllocationLatencyPercentile(90),
//                                operationHandlerPool.getAllocationLatencyPercentile(99),
//                                operationHandlerPool.getAllocationLatencyPercentile(100),
//                                operationHandlerPool.getDeallocationLatencyPercentile(90),
//                                operationHandlerPool.getDeallocationLatencyPercentile(99),
//                                operationHandlerPool.getDeallocationLatencyPercentile(100),
//                                operationHandlerPool.getAllocationFailureLatencyPercentile(90),
//                                operationHandlerPool.getAllocationFailureLatencyPercentile(99),
//                                operationHandlerPool.getAllocationFailureLatencyPercentile(100),
//                                operationHandlerPool.getObjectLifetimePercentile(90),
//                                operationHandlerPool.getObjectLifetimePercentile(99),
//                                operationHandlerPool.getObjectLifetimePercentile(100),
//                                operationHandlerPool.getReallocationFailureLatencyPercentile(90),
//                                operationHandlerPool.getReallocationFailureLatencyPercentile(99),
//                                operationHandlerPool.getReallocationFailureLatencyPercentile(100)
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