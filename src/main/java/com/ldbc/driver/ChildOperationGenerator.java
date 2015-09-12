package com.ldbc.driver;

public interface ChildOperationGenerator
{
    double initialState();

    Operation nextOperation(
            double state,
            Operation operation,
            Object result,
            long actualStartTimeAsMilli,
            long runDurationAsNano ) throws WorkloadException;

    double updateState(
            double previousState,
            int previousOperationType );
}
