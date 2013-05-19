package com.ldbc.db2;

import com.ldbc.measurements.Measurements;

public abstract class OperationHandler2<A extends Operation2<?>>
{
    private final Measurements measurements = Measurements.getMeasurements();

    public final OperationResult2 execute( Operation2<?> operation )
    {
        long startTime = System.nanoTime();

        OperationResult2 operationResult = executeOperation( (A) operation );

        long endTime = System.nanoTime();

        String operationType = operation.getClass().getName();
        measurements.measure( operationType, (int) ( ( endTime - startTime ) / 1000 ) );
        measurements.reportReturnCode( operationType, operationResult.getResultCode() );

        return operationResult;
    }

    protected abstract OperationResult2 executeOperation( A operation );
}
