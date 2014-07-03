package com.ldbc.driver;

import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.temporal.Duration;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class Workload {
    public static final Duration DEFAULT_MAXIMUM_EXPECTED_INTERLEAVE = Duration.fromMinutes(30);

    public static final Class<Operation<?>>[] operationTypesBySchedulingMode(Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications,
                                                                             OperationClassification.SchedulingMode schedulingMode) {
        List<Class<? extends Operation<?>>> operationsBySchedulingMode = new ArrayList<>();
        for (Map.Entry<Class<? extends Operation<?>>, OperationClassification> operationAndClassification : operationClassifications.entrySet()) {
            if (operationAndClassification.getValue().schedulingMode().equals(schedulingMode))
                operationsBySchedulingMode.add(operationAndClassification.getKey());
        }
        return operationsBySchedulingMode.toArray(new Class[operationsBySchedulingMode.size()]);
    }

    private boolean isInitialized = false;
    private boolean isCleanedUp = false;

    public abstract Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications();

    /**
     * Called once to initialize state for workload
     */
    public final void init(DriverConfiguration params) throws WorkloadException {
        if (isInitialized) {
            throw new WorkloadException("Workload may be initialized only once");
        }
        isInitialized = true;
        onInit(params.asMap());
    }

    public abstract void onInit(Map<String, String> params) throws WorkloadException;

    public final void cleanup() throws WorkloadException {
        if (isCleanedUp) {
            throw new WorkloadException("Workload may be cleaned up only once");
        }
        isCleanedUp = true;
        onCleanup();
    }

    protected abstract void onCleanup() throws WorkloadException;

    // TODO should this method take start time and compression ratio as input and do compression + offset?
    public final Iterator<Operation<?>> operations(GeneratorFactory generators, long operationCount)
            throws WorkloadException {
        if (false == isInitialized) {
            throw new WorkloadException("Workload has not been initialized");
        }
        return generators.limit(createOperations(generators), operationCount);
    }

    protected abstract Iterator<Operation<?>> createOperations(GeneratorFactory generators)
            throws WorkloadException;

    public boolean validationResultCheck(Operation<?> operation, Object operationResult) {
        return true;
    }

    public Duration maxExpectedInterleave() {
        return DEFAULT_MAXIMUM_EXPECTED_INTERLEAVE;
    }

    public abstract String serializeOperation(Operation<?> operation) throws SerializingMarshallingException;

    public abstract Operation<?> marshalOperation(String serializedOperation) throws SerializingMarshallingException;
}