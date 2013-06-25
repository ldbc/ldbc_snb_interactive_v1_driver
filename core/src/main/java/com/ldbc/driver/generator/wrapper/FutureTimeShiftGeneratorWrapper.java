package com.ldbc.driver.generator.wrapper;

import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.util.Duration;
import com.ldbc.driver.util.Time;

/*
 * Offset is initialized the first time next() is called
 */
public class FutureTimeShiftGeneratorWrapper extends Generator<Operation<?>>
{
    private final Generator<Operation<?>> operationGenerator;
    private Duration offsetDuration = Duration.fromNano( -1 );

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

        if ( operation.getScheduledStartTime() == Operation.UNASSIGNED_SCHEDULED_START_TIME )
        {
            throw new GeneratorException( "Operation must have a scheduled start time" );
        }

        // TODO performing check every time is shit. FIX
        if ( -1 == offsetDuration.asNano() )
        {
            offsetDuration = calculateOffsetDuration( operation );
        }

        operation.setScheduledStartTime( operation.getScheduledStartTime().plus( offsetDuration ) );
        return operation;
    }

    private Duration calculateOffsetDuration( Operation<?> firstOperation )
    {
        Duration offset = Duration.durationBetween( Time.now(), firstOperation.getScheduledStartTime() );
        if ( offset.asNano() < 0 )
        {
            throw new GeneratorException( "Scheduled start time of first operation must be in the past" );
        }
        return offset;
    }
}
