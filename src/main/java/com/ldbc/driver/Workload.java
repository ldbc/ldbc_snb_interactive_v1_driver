package com.ldbc.driver;

import com.ldbc.driver.validation.DbValidationParametersFilter;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.validation.ResultsLogValidationTolerances;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class Workload implements Closeable {

    public static final long DEFAULT_MAXIMUM_EXPECTED_INTERLEAVE_AS_MILLI = TimeUnit.HOURS.toMillis(1);
    private boolean isInitialized = false;
    private boolean isClosed = false;


    public abstract Map<Integer, Class<? extends Operation>> operationTypeToClassMapping();

    // TODO: something for result log?
    public ResultsLogValidationTolerances resultsLogValidationTolerances(
            DriverConfiguration configuration,
            boolean warmup
    ) {
        long excessiveDelayThresholdAsMilli = TimeUnit.SECONDS.toMillis(1);
        double toleratedExcessiveDelayCountPercentage = 0.01;
        long toleratedExcessiveDelayCount =
                (warmup) ? Math.round(configuration.warmupCount() * toleratedExcessiveDelayCountPercentage)
                        : Math.round(configuration.getOperationCount() * toleratedExcessiveDelayCountPercentage);
        // TODO this should really be percentages instead of absolute numbers
        Map<String, Long> toleratedExcessiveDelayCountPerType = new HashMap<>();
        for (Class operationType : operationTypeToClassMapping().values()) {
            toleratedExcessiveDelayCountPerType.put(operationType.getSimpleName(), 10L);
        }
        return new ResultsLogValidationTolerances(
                excessiveDelayThresholdAsMilli,
                toleratedExcessiveDelayCount,
                toleratedExcessiveDelayCountPerType
        );
    }

    /**
     * Called once to initialize state for workload
     * @param params driver configuration
     * @throws WorkloadException workload exception
     */
    public final void init(DriverConfiguration params) throws WorkloadException {
        if (isInitialized) {
            throw new WorkloadException("Workload may be initialized only once");
        }
        isInitialized = true;
        onInit(params.asMap());
    }

    public abstract void onInit(Map<String, String> params) throws WorkloadException;

    /**
     * Called once to close workload and release resources
     * @throws IOException io exception
     */
    public final void close() throws IOException {
        if (isClosed) {
            throw new IOException("Workload may be cleaned up only once");
        }
        isClosed = true;
        onClose();
    }

    protected abstract void onClose() throws IOException;

    /**
     * Get workload streams
     * @param gf random number generator factory
     * @param hasDbConnected if database has been connected to
     * @return workload streams
     * @throws WorkloadException workload exception
     */
    public final WorkloadStreams streams(GeneratorFactory gf, boolean hasDbConnected) throws WorkloadException {
        if (!isInitialized) {
            throw new WorkloadException("Workload has not been initialized");
        }
        return getStreams(gf, hasDbConnected);
    }

    protected abstract WorkloadStreams getStreams(GeneratorFactory generators, boolean hasDbConnected)
            throws WorkloadException;

    public abstract DbValidationParametersFilter getDbValidationParametersFilter(int requiredValidationParameterCount);

    public long maxExpectedInterleaveAsMilli() {
        return DEFAULT_MAXIMUM_EXPECTED_INTERLEAVE_AS_MILLI;
    }

    public abstract String serializeOperation(Operation operation) throws SerializingMarshallingException;

    public abstract Operation marshalOperation(String serializedOperation) throws SerializingMarshallingException;

    public abstract boolean resultsEqual(Operation operation, Object result1, Object result2)
            throws WorkloadException;

}