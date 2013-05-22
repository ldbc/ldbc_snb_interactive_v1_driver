package com.ldbc.driver;

import com.ldbc.driver.measurements.Measurements;

public abstract class OperationHandler<A extends Operation<?>>
{
    // TODO change the way this is done! no static
    private final Measurements measurements = Measurements.getMeasurements();

    private A operation;

    public final void setOperation( Operation<?> operation )
    {
        this.operation = (A) operation;
    }

    final OperationResult execute()
    {
        long actualStartTime = System.nanoTime();

        OperationResult operationResult = executeOperation( operation );

        long actualEndTime = System.nanoTime();

        String operationType = operation.getClass().getName();
        // TODO report scheduleStartTime
        // TODO report actualStartTime
        // TODO report duration (why is /1000 necessary?)
        measurements.measure( operationType, (int) ( ( actualEndTime - actualStartTime ) / 1000 ) );
        measurements.reportReturnCode( operationType, operationResult.getResultCode() );

        return operationResult;
    }

    protected abstract OperationResult executeOperation( A operation );
}
