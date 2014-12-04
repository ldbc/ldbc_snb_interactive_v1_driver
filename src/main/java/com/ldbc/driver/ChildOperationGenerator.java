package com.ldbc.driver;

public interface ChildOperationGenerator {
    double initialState();

    boolean hasNext(double state);

    Operation<?> nextOperation(OperationResultReport resultReport);

    double updateState(double state);
}
