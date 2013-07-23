package com.ldbc.driver.generator.wrapper;

import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.util.Function;
import com.ldbc.driver.util.temporal.Duration;
import com.ldbc.driver.util.temporal.Time;

public class FutureTimeShiftGeneratorWrapper extends Generator<Operation<?>>
{
    private final Generator<Operation<?>> operationGenerator;
    private Operation<?> firstOperation;

    public FutureTimeShiftGeneratorWrapper( Generator<Operation<?>> operationGenerator, Time startTime )
    {
        super( null );
        firstOperation = operationGenerator.next();
        Duration offsetDuration = startTime.greaterBy( firstOperation.getScheduledStartTime() );
        // TODO remove if pass
        // Duration offsetDuration = Duration.durationTo(
        // firstOperation.getScheduledStartTime(), startTime );
        Function<Operation<?>, Operation<?>> timeShiftFun = new TimeShiftFunction( offsetDuration );
        firstOperation = timeShiftFun.apply( firstOperation );
        this.operationGenerator = new MapGeneratorWrapper<Operation<?>, Operation<?>>( operationGenerator,
                new TimeShiftFunction( offsetDuration ) );
    }

    @Override
    protected Operation<?> doNext() throws GeneratorException
    {
        if ( null != firstOperation )
        {
            Operation<?> next = firstOperation;
            firstOperation = null;
            return next;
        }
        if ( false == operationGenerator.hasNext() )
        {
            return null;
        }
        Operation<?> operation = operationGenerator.next();
        if ( operation.getScheduledStartTime() == Operation.UNASSIGNED_SCHEDULED_START_TIME )
        {
            throw new GeneratorException( "Original Operation must have a scheduled start time" );
        }
        return operation;
    }

    static class TimeShiftFunction implements Function<Operation<?>, Operation<?>>
    {
        final Duration offsetDuration;

        TimeShiftFunction( Duration offsetDuration )
        {
            this.offsetDuration = offsetDuration;
        }

        @Override
        public Operation<?> apply( Operation<?> operation )
        {
            operation.setScheduledStartTime( operation.getScheduledStartTime().plus( offsetDuration ) );
            return operation;
        }
    };
}
