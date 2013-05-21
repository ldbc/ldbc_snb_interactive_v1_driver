package com.ldbc;

import com.ldbc.measurements.Measurements;

public abstract class OperationHandler<A extends Operation<?>>
{
    // TODO change the way this is done! no static
    private final Measurements measurements = Measurements.getMeasurements();

    private A operation = null;

    final void setOperation( Operation<?> operation )
    {
        this.operation = (A) operation;
    }

    final OperationResult execute()
    {
        long startTime = System.nanoTime();

        OperationResult operationResult = executeOperation( operation );

        long endTime = System.nanoTime();

        String operationType = operation.getClass().getName();
        measurements.measure( operationType, (int) ( ( endTime - startTime ) / 1000 ) );
        measurements.reportReturnCode( operationType, operationResult.getResultCode() );

        return operationResult;
    }

    protected abstract OperationResult executeOperation( A operation );
}
