package com.ldbc.db;

import com.ldbc.measurements.Measurements;

public abstract class OperationHandler<A extends Operation<?>>
{
    private final Measurements measurements = Measurements.getMeasurements();

    public final OperationResult execute( Operation<?> operation )
    {
        long startTime = System.nanoTime();

        OperationResult operationResult = executeOperation( (A) operation );

        long endTime = System.nanoTime();

        String operationType = operation.getClass().getName();
        measurements.measure( operationType, (int) ( ( endTime - startTime ) / 1000 ) );
        measurements.reportReturnCode( operationType, operationResult.getResultCode() );

        return operationResult;
    }

    protected abstract OperationResult executeOperation( A operation );
}
