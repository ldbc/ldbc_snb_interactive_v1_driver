package com.ldbc.driver.runtime.metrics;

interface MetricsCollectionEventFactory {
    MetricsCollectionEvent newMetricsCollectionEvent() throws MetricsCollectionException;

    void shutdown() throws MetricsCollectionException;
}