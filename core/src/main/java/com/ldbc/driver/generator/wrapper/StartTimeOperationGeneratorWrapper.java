package com.ldbc.driver.generator.wrapper;

import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.util.temporal.Time;

public class StartTimeOperationGeneratorWrapper extends Generator<Operation<?>>
{
    private final Generator<Time> startTimeGenerator;
    private final Generator<Operation<?>> operationGenerator;

    public StartTimeOperationGeneratorWrapper( Generator<Time> startTimeGenerator,
            Generator<Operation<?>> operationGenerator )
    {
        this.startTimeGenerator = startTimeGenerator;
        this.operationGenerator = operationGenerator;
    }

    @Override
    protected Operation<?> doNext() throws GeneratorException
    {
        if ( operationGenerator.hasNext() && startTimeGenerator.hasNext() )
        {
            Operation<?> operation = operationGenerator.next();
            operation.setScheduledStartTime( startTimeGenerator.next() );
            return operation;
        }
        return null;
    }
}
