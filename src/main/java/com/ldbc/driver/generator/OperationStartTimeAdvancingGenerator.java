package com.ldbc.driver.generator;

import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.Function1;

import java.util.Iterator;

/*
 * Advances the scheduled start time of an Operation by a given Duration 
 */
public class OperationStartTimeAdvancingGenerator extends Generator<Operation<?>> {
    private final Iterator<Operation<?>> operationGenerator;
    private Operation<?> firstOperation;

    OperationStartTimeAdvancingGenerator(Iterator<Operation<?>> operationGenerator, Time startTime) {
        firstOperation = operationGenerator.next();
        Duration offsetDuration = startTime.greaterBy(firstOperation.scheduledStartTime());
        Function1<Operation<?>, Operation<?>> timeShiftFun = new TimeShiftFunction(offsetDuration);
        firstOperation = timeShiftFun.apply(firstOperation);
        this.operationGenerator = new MappingGenerator<Operation<?>, Operation<?>>(operationGenerator,
                new TimeShiftFunction(offsetDuration));
    }

    @Override
    protected Operation<?> doNext() throws GeneratorException {
        if (null != firstOperation) {
            Operation<?> next = firstOperation;
            firstOperation = null;
            return next;
        }
        if (false == operationGenerator.hasNext()) {
            return null;
        }
        Operation<?> operation = operationGenerator.next();
        if (null == operation.scheduledStartTime()) {
            throw new GeneratorException("Original Operation must have a scheduled start time");
        }
        return operation;
    }

    static class TimeShiftFunction implements Function1<Operation<?>, Operation<?>> {
        final Duration offsetDuration;

        TimeShiftFunction(Duration offsetDuration) {
            this.offsetDuration = offsetDuration;
        }

        @Override
        public Operation<?> apply(Operation<?> operation) {
            operation.setScheduledStartTime(operation.scheduledStartTime().plus(offsetDuration));
            return operation;
        }
    }

    ;
}
