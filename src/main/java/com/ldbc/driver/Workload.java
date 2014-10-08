package com.ldbc.driver;

import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.temporal.Duration;

import java.util.Map;

public abstract class Workload {
    public static final Duration DEFAULT_MAXIMUM_EXPECTED_INTERLEAVE = Duration.fromMinutes(60);

    private boolean isInitialized = false;
    private boolean isCleanedUp = false;

    /**
     * Called once to initialize state for workload
     */
    public final void init(DriverConfiguration params) throws WorkloadException {
        if (isInitialized)
            throw new WorkloadException("Workload may be initialized only once");
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

    public final WorkloadStreams streams(GeneratorFactory gf) throws WorkloadException {
        if (false == isInitialized)
            throw new WorkloadException("Workload has not been initialized");
        return getStreams(gf);
    }

    protected abstract WorkloadStreams getStreams(GeneratorFactory generators) throws WorkloadException;

    public DbValidationParametersFilter dbValidationParametersFilter(final Integer requiredValidationParameterCount) {
        return new DbValidationParametersFilter() {
            int validationParameterCount = 0;

            @Override
            public boolean useOperation(Operation<?> operation) {
                return true;
            }

            @Override
            public DbValidationParametersFilterResult useOperationAndResultForValidation(Operation<?> operation, Object operationResult) {
                if (validationParameterCount < requiredValidationParameterCount) {
                    validationParameterCount++;
                    return DbValidationParametersFilterResult.ACCEPT_AND_CONTINUE;
                }
                return DbValidationParametersFilterResult.REJECT_AND_FINISH;
            }
        };
    }

    public Duration maxExpectedInterleave() {
        return DEFAULT_MAXIMUM_EXPECTED_INTERLEAVE;
    }

    public abstract String serializeOperation(Operation<?> operation) throws SerializingMarshallingException;

    public abstract Operation<?> marshalOperation(String serializedOperation) throws SerializingMarshallingException;

    public static interface DbValidationParametersFilter {
        boolean useOperation(Operation<?> operation);

        DbValidationParametersFilterResult useOperationAndResultForValidation(Operation<?> operation, Object operationResult);
    }

    public enum DbValidationParametersFilterResult {
        ACCEPT_AND_CONTINUE,
        ACCEPT_AND_FINISH,
        REJECT_AND_CONTINUE,
        REJECT_AND_FINISH
    }

}