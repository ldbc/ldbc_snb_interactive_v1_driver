package com.ldbc.driver.runtime.metrics;

import stormpot.Poolable;
import stormpot.Slot;

// TODO metrics poolable object
// OperationResultReport(int resultCode, Object operationResult, Operation<?> operation) {

class MetricsCollectionEvent implements Poolable {

    public static enum MetricsEventType {
        // Submit operation result for its metrics to be collected
        SUBMIT_RESULT,
        // Request metrics summary
        WORKLOAD_STATUS,
        // Request complete workload results
        WORKLOAD_RESULT,
        // Terminate when all results metrics have been collected
        TERMINATE_SERVICE
    }

    private Slot slot;
    private MetricsEventType type;
    private Object value;

    public void setSlot(Slot slot) {
        this.slot = slot;
    }

    public MetricsEventType type() {
        return type;
    }

    public Object value() {
        return value;
    }

    public void setType(MetricsEventType type) {
        this.type = type;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public void release() {
        type = null;
        value = null;
        if (null != slot)
            slot.release(this);
    }
}