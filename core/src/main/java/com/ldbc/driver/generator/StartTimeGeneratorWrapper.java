package com.ldbc.driver.generator;

import com.ldbc.driver.Operation;

public class StartTimeGeneratorWrapper extends Generator<Operation<?>>
{
    private final Generator<Long> startTimeGenerator;
    private final Generator<Operation<?>> operationGenerator;

    public StartTimeGeneratorWrapper( Generator<Long> startTimeGenerator, Generator<Operation<?>> operationGenerator )
    {
        super( startTimeGenerator.getRandom() );
        this.startTimeGenerator = startTimeGenerator;
        this.operationGenerator = operationGenerator;
    }

    @Override
    protected Operation<?> doNext() throws GeneratorException
    {
        Operation<?> operation = operationGenerator.next();
        operation.setScheduledStartTime( startTimeGenerator.next() );
        return operation;
    }
}
