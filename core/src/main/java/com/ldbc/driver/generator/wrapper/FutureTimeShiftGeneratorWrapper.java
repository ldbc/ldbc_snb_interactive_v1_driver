package com.ldbc.driver.generator.wrapper;

import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;

/*
 * Offset is initialized the first time next() is called
 */
public class FutureTimeShiftGeneratorWrapper extends Generator<Operation<?>>
{
    private final Generator<Operation<?>> operationGenerator;
    private long scheduleStartTimeOffset = -1;

    public FutureTimeShiftGeneratorWrapper( Generator<Operation<?>> operationGenerator )
    {
        super( null );
        this.operationGenerator = operationGenerator;
    }

    @Override
    protected Operation<?> doNext() throws GeneratorException
    {
        if ( false == operationGenerator.hasNext() ) return null;
        Operation<?> operation = operationGenerator.next();

        if ( operation.getScheduledStartTimeNanoSeconds() == Operation.UNASSIGNED_SCHEDULED_START_TIME )
        {
            throw new GeneratorException( "Operation must have a scheduled start time" );
        }

        // TODO performing check every time is shit. FIX
        if ( -1 == scheduleStartTimeOffset )
        {
            scheduleStartTimeOffset = calculateScheduledStartTimeOffset( operation );
        }

        operation.setScheduledStartTimeNanoSeconds( operation.getScheduledStartTimeNanoSeconds()
                                                    + scheduleStartTimeOffset );
        return operation;
    }

    private long calculateScheduledStartTimeOffset( Operation<?> firstOperation )
    {
        long offset = System.nanoTime() - firstOperation.getScheduledStartTimeNanoSeconds();
        if ( offset < 0 )
        {
            throw new GeneratorException( "Scheduled start time of first operation must be in the past" );
        }
        return offset;
    }
}
