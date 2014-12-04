package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.ChildOperationGenerator;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationResultReport;

public class LdbcSnbShortReadGenerator implements ChildOperationGenerator {
    private final double initialProbability;
    private final double probabilityDegradationFactor;
    private final double minimumProbability;

    public LdbcSnbShortReadGenerator(double initialProbability,
                                     double probabilityDegradationFactor,
                                     double minimumProbability) {
        this.initialProbability = initialProbability;
        this.probabilityDegradationFactor = probabilityDegradationFactor;
        this.minimumProbability = minimumProbability;
    }

    @Override
    public double initialState() {
        return initialProbability;
    }

    @Override
    public boolean hasNext(double state) {
        return state > minimumProbability;
    }

    @Override
    public Operation<?> nextOperation(OperationResultReport resultReport) {
        // TODO
        return null;
    }

    @Override
    public double updateState(double state) {
        return state * probabilityDegradationFactor;
    }
}
