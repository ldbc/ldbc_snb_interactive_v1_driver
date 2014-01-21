package com.ldbc.driver;

import com.google.common.collect.ImmutableList;
import com.ldbc.driver.generator.GeneratorFactory;

import java.util.Iterator;
import java.util.Map;

public abstract class Workload {
    private boolean isInitialized = false;
    private boolean isCleanedUp = false;

    private long operationCount;
    private long recordCount;

    /**
     * Called once to initialize state for workload
     */
    public final void init(WorkloadParams params) throws WorkloadException {
        if (true == isInitialized) {
            throw new WorkloadException("Workload may be initialized only once");
        }
        isInitialized = true;
        this.operationCount = params.operationCount();
        this.recordCount = params.recordCount();
        onInit(params.asMap());
    }

    protected long getOperationCount() {
        return operationCount;
    }

    protected long getRecordCount() {
        return recordCount;
    }

    public abstract void onInit(Map<String, String> properties) throws WorkloadException;

    public final void cleanup() throws WorkloadException {
        if (true == isCleanedUp) {
            throw new WorkloadException("Workload may be cleaned up only once");
        }
        isCleanedUp = true;
        onCleanup();
    }

    protected abstract void onCleanup() throws WorkloadException;

    public final Iterator<Operation<?>> getLoadOperations(GeneratorFactory generators) throws WorkloadException {
        if (WorkloadParams.UNBOUNDED_OPERATION_COUNT == getOperationCount()) {
            // Generate all workload operations before beginning
            return ImmutableList.copyOf(createLoadOperations(generators)).iterator();
        } else {
            // Generate all workload operations before beginning
            return ImmutableList.copyOf(generators.limit(createLoadOperations(generators), getOperationCount())).iterator();
        }
    }

    protected abstract Iterator<Operation<?>> createLoadOperations(GeneratorFactory generators)
            throws WorkloadException;

    public final Iterator<Operation<?>> getTransactionalOperations(GeneratorFactory generators)
            throws WorkloadException {
        if (WorkloadParams.UNBOUNDED_OPERATION_COUNT == getOperationCount()) {
            // Generate all workload operations before beginning
            return ImmutableList.copyOf(createTransactionalOperations(generators)).iterator();
        } else {
            // Generate all workload operations before beginning
            return ImmutableList.copyOf(
                    generators.limit(createTransactionalOperations(generators), getOperationCount())).iterator();
        }
    }

    protected abstract Iterator<Operation<?>> createTransactionalOperations(GeneratorFactory generators)
            throws WorkloadException;
}
