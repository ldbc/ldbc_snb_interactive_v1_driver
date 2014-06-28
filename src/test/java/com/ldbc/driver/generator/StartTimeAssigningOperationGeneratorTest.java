package com.ldbc.driver.generator;

import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.testutils.NothingOperation;
import com.ldbc.driver.util.Function1;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import org.junit.Test;

import java.util.Iterator;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class StartTimeAssigningOperationGeneratorTest {
    @Test
    public void shouldAssignStartTimesToOperations() {
        // Given
        long firstNanoTime = 1000;
        long incrementNanoTimeBy = 100;
        int testIterations = 10;
        GeneratorFactory generators = new GeneratorFactory(new RandomDataGeneratorFactory(42l));

        // When
        Iterator<Operation<?>> operationGenerator = generators.limit(new OperationGenerator(), testIterations);
        Function1<Long, Time> timeFromLongFun = new Function1<Long, Time>() {
            @Override
            public Time apply(Long from) {
                return Time.fromNano(from);
            }
        };

        Iterator<Long> countGenerator = generators.incrementing(firstNanoTime, incrementNanoTimeBy);
        Iterator<Time> counterStartTimeGenerator = generators.map(countGenerator, timeFromLongFun);
        Iterator<Operation<?>> startTimeOperationGenerator = generators.startTimeAssigning(counterStartTimeGenerator, operationGenerator);

        // Then
        int count = 0;
        Time lastTime = Time.fromNano(firstNanoTime).minus(Duration.fromNano(incrementNanoTimeBy));
        while (startTimeOperationGenerator.hasNext()) {
            Operation<?> operation = startTimeOperationGenerator.next();
            assertThat(operation.scheduledStartTime(), is(lastTime.plus(Duration.fromNano(incrementNanoTimeBy))));
            lastTime = operation.scheduledStartTime();
            count++;
        }
        assertThat(count, is(testIterations));
    }

    static class OperationGenerator extends Generator<Operation<?>> {
        @Override
        protected Operation<?> doNext() throws GeneratorException {
            return new NothingOperation();
        }
    }
}
