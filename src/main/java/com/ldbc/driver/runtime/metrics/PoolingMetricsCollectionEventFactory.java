package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.temporal.TemporalUtil;
import stormpot.*;

import java.util.concurrent.TimeUnit;

class PoolingMetricsCollectionEventFactory implements MetricsCollectionEventFactory {
    private static final Timeout POOL_CLAIM_TIMEOUT = new Timeout(100, TimeUnit.MILLISECONDS);
    private static final Timeout POOL_CLAIM_AFTER_RESIZE_TIMEOUT = new Timeout(1000, TimeUnit.MILLISECONDS);
    private static final Timeout POOL_SHUTDOWN_TIMEOUT = new Timeout(10, TimeUnit.SECONDS);
    private final LifecycledResizablePool<MetricsCollectionEvent> metricsEventPool;
    private final MetricsCollectionEventFactory innerMetricsEventFactory;

    PoolingMetricsCollectionEventFactory(MetricsCollectionEventFactory innerMetricsEventFactory) {
        this.innerMetricsEventFactory = innerMetricsEventFactory;
        MetricsEventAllocator metricsEventAllocator = new MetricsEventAllocator(innerMetricsEventFactory);
        Config<MetricsCollectionEvent> metricsEventPoolConfig = new Config<>();
        metricsEventPoolConfig.setAllocator(metricsEventAllocator);
        this.metricsEventPool = new BlazePool<>(metricsEventPoolConfig);
        this.metricsEventPool.setTargetSize(64);

    }

    @Override
    public MetricsCollectionEvent newMetricsCollectionEvent() throws MetricsCollectionException {
        try {
            MetricsCollectionEvent metricsCollectionEvent = metricsEventPool.claim(POOL_CLAIM_TIMEOUT);
            while (null == metricsCollectionEvent) {
                metricsEventPool.setTargetSize(metricsEventPool.getTargetSize() * 2);
                metricsCollectionEvent = metricsEventPool.claim(POOL_CLAIM_AFTER_RESIZE_TIMEOUT);
            }
            return metricsCollectionEvent;
        } catch (InterruptedException e) {
            throw new MetricsCollectionException("Error encountered while attempting to allocate metrics event from pool", e);
        }
    }

    public void shutdown() throws MetricsCollectionException {
        TemporalUtil temporalUtil = new TemporalUtil();
        innerMetricsEventFactory.shutdown();
        Completion completion = metricsEventPool.shutdown();
        try {
            boolean isSuccessfulShutdown = completion.await(POOL_SHUTDOWN_TIMEOUT);
            if (false == isSuccessfulShutdown)
                throw new MetricsCollectionException(
                        String.format(
                                "Metrics event pool did not shutdown before timeout (%s)",
                                temporalUtil.milliDurationToString(temporalUtil.convert(POOL_SHUTDOWN_TIMEOUT.getTimeout(), POOL_SHUTDOWN_TIMEOUT.getUnit(), TimeUnit.MILLISECONDS))
                        )
                );
        } catch (InterruptedException e) {
            throw new MetricsCollectionException("Error encountered while shutting down metrics event pool", e);
        }
    }

    private static class MetricsEventAllocator implements Allocator<MetricsCollectionEvent> {
        private final MetricsCollectionEventFactory metricsCollectionEventFactory;

        public MetricsEventAllocator(MetricsCollectionEventFactory metricsCollectionEventFactory) {
            this.metricsCollectionEventFactory = metricsCollectionEventFactory;
        }

        @Override
        public MetricsCollectionEvent allocate(Slot slot) throws Exception {
            MetricsCollectionEvent metricsCollectionEvent = metricsCollectionEventFactory.newMetricsCollectionEvent();
            metricsCollectionEvent.setSlot(slot);
            return metricsCollectionEvent;
        }

        @Override
        public void deallocate(MetricsCollectionEvent event) throws Exception {
            // I think nothing needs to be done here
        }
    }
}
