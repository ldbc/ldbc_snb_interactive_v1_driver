package com.ldbc.driver;

public interface ChildOperationGenerator {
    double initialState();

    Operation<?> nextOperation(double state, OperationResultReport resultReport) throws WorkloadException;

    double updateState(double state);
}
