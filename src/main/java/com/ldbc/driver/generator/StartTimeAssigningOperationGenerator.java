package com.ldbc.driver.generator;

import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.Time;

import java.util.Iterator;

public class StartTimeAssigningOperationGenerator extends Generator<Operation<?>> {
    private final Iterator<Time> startTimeGenerator;
    private final Iterator<Operation<?>> operationGenerator;

    public StartTimeAssigningOperationGenerator(Iterator<Time> startTimeGenerator,
                                                Iterator<Operation<?>> operationGenerator) {
        this.startTimeGenerator = startTimeGenerator;
        this.operationGenerator = operationGenerator;
    }

    @Override
    protected Operation<?> doNext() throws GeneratorException {
        if (operationGenerator.hasNext() && startTimeGenerator.hasNext()) {
            Operation<?> operation = operationGenerator.next();
            operation.setScheduledStartTime(startTimeGenerator.next());
            return operation;
        }
        return null;
    }
}
