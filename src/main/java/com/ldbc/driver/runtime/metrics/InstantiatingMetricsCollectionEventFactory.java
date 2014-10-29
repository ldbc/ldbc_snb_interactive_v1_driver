package com.ldbc.driver.runtime.metrics;

import stormpot.Poolable;
import stormpot.Slot;

class InstantiatingMetricsCollectionEventFactory implements MetricsCollectionEventFactory {
    private static final Slot DUMMY_SLOT = new Slot() {
        @Override
        public void release(Poolable obj) {
            // do nothing
        }
    };

    @Override
    public MetricsCollectionEvent newMetricsCollectionEvent() throws MetricsCollectionException {
        MetricsCollectionEvent event = new MetricsCollectionEvent();
        event.setSlot(DUMMY_SLOT);
        return event;
    }

    public void shutdown() throws MetricsCollectionException {
        // do nothing
    }

}
