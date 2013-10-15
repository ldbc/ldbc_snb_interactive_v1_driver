package com.ldbc.driver.generator.wrapper;

import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.generator.MappingGenerator;
import com.ldbc.driver.util.Function1;
import com.ldbc.driver.util.temporal.Duration;
import com.ldbc.driver.util.temporal.Time;

public class FutureTimeShiftGeneratorWrapper extends Generator<Operation<?>>
{
    private final Generator<Operation<?>> operationGenerator;
    private Operation<?> firstOperation;

    public FutureTimeShiftGeneratorWrapper( Generator<Operation<?>> operationGenerator, Time startTime )
    {
        firstOperation = operationGenerator.next();
        Duration offsetDuration = startTime.greaterBy( firstOperation.getScheduledStartTime() );
        Function1<Operation<?>, Operation<?>> timeShiftFun = new TimeShiftFunction( offsetDuration );
        firstOperation = timeShiftFun.apply( firstOperation );
        this.operationGenerator = new MappingGenerator<Operation<?>, Operation<?>>( operationGenerator,
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

    static class TimeShiftFunction implements Function1<Operation<?>, Operation<?>>
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
