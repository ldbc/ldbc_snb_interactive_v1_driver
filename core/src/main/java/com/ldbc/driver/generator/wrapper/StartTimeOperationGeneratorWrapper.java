package com.ldbc.driver.generator.wrapper;

import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;

public class StartTimeOperationGeneratorWrapper extends Generator<Operation<?>>
{
    private final Generator<Long> startTimeGenerator;
    private final Generator<Operation<?>> operationGenerator;

    public StartTimeOperationGeneratorWrapper( Generator<Long> startTimeGenerator, Generator<Operation<?>> operationGenerator )
    {
        super( null );
        this.startTimeGenerator = startTimeGenerator;
        this.operationGenerator = operationGenerator;
    }

    @Override
    protected Operation<?> doNext() throws GeneratorException
    {
        if ( false == operationGenerator.hasNext() ) return null;
        if ( false == startTimeGenerator.hasNext() ) return null;
        Operation<?> operation = operationGenerator.next();
        operation.setScheduledStartTimeNanoSeconds( startTimeGenerator.next() );
        return operation;
    }
}
