package com.ldbc.driver.generator.wrapper;

import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.util.Duration;
import com.ldbc.driver.util.Time;

public class FutureTimeShiftGeneratorWrapper extends Generator<Operation<?>>
{
    private final Generator<Operation<?>> operationGenerator;
    private final Duration offsetDuration;
    private Operation<?> firstOperation;

    public FutureTimeShiftGeneratorWrapper( Generator<Operation<?>> operationGenerator, Time startTime )
    {
        super( null );
        this.operationGenerator = operationGenerator;
        this.firstOperation = this.operationGenerator.next();
        this.offsetDuration = Duration.durationBetween( firstOperation.getScheduledStartTime(), startTime );
    }

    @Override
    protected Operation<?> doNext() throws GeneratorException
    {
        if ( null != firstOperation )
        {
            Operation<?> next = offsetOperationScheduledStartTime( firstOperation );
            firstOperation = null;
            return next;
        }
        if ( false == operationGenerator.hasNext() ) return null;
        Operation<?> operation = operationGenerator.next();

        if ( operation.getScheduledStartTime() == Operation.UNASSIGNED_SCHEDULED_START_TIME )
        {
            throw new GeneratorException( "Original Operation must have a scheduled start time" );
        }

        return offsetOperationScheduledStartTime( operation );
    }

    private Operation<?> offsetOperationScheduledStartTime( Operation<?> operation )
    {
        operation.setScheduledStartTime( operation.getScheduledStartTime().plus( offsetDuration ) );
        return operation;
    }
}
