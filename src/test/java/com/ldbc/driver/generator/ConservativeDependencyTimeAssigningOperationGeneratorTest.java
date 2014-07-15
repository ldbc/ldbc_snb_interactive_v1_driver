package com.ldbc.driver.generator;

import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.testutils.TimedOperation;
import com.ldbc.driver.util.Function1;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import org.junit.Test;

import java.util.Iterator;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ConservativeDependencyTimeAssigningOperationGeneratorTest {
    @Test
    public void shouldAssignStartTimesToOperations() {
        // Given
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));
        Time startTime = Time.fromMilli(0);
        Operation<?> operation1Dependent = new TimedOperation(Time.fromMilli(1), null);
        final Operation<?> operation2Dependency = new TimedOperation(Time.fromMilli(2), null);
        Operation<?> operation3Dependent = new TimedOperation(Time.fromMilli(2), null);
        final Operation<?> operation4Dependency = new TimedOperation(Time.fromMilli(3), null);
        Operation<?> operation5Dependent = new TimedOperation(Time.fromMilli(4), null);
        Operation<?> operation6Dependent = new TimedOperation(Time.fromMilli(5), null);
        Iterator<Operation<?>> operations = Lists.newArrayList(
                operation1Dependent,
                operation2Dependency,
                operation3Dependent,
                operation4Dependency,
                operation5Dependent,
                operation6Dependent
        ).iterator();
        Function1<Operation<?>, Boolean> isDependency = new Function1<Operation<?>, Boolean>() {
            @Override
            public Boolean apply(Operation<?> operation) {
                return operation.equals(operation2Dependency) || operation.equals(operation4Dependency);
            }
        };
        boolean canOverwriteDependencyTime = true;

        // When
        Iterator<Operation<?>> operationsWithDependencyTimes =
                gf.assignConservativeDependencyTimes(operations, isDependency, startTime, canOverwriteDependencyTime);

        // Then

        // Dependent
        Operation<?> operation1 = operationsWithDependencyTimes.next();
        assertThat(operation1, equalTo((Operation) operation1Dependent));
        assertThat(operation1.scheduledStartTime(), equalTo(Time.fromMilli(1)));
        assertThat(operation1.dependencyTime(), equalTo(Time.fromMilli(0)));

        // Dependency
        Operation<?> operation2 = operationsWithDependencyTimes.next();
        assertThat(operation2, equalTo((Operation) operation2Dependency));
        assertThat(operation2.scheduledStartTime(), equalTo(Time.fromMilli(2)));
        assertThat(operation2.dependencyTime(), equalTo(Time.fromMilli(0)));

        // Dependent
        Operation<?> operation3 = operationsWithDependencyTimes.next();
        assertThat(operation3, equalTo((Operation) operation3Dependent));
        assertThat(operation3.scheduledStartTime(), equalTo(Time.fromMilli(2)));
        assertThat(operation3.dependencyTime(), equalTo(Time.fromMilli(2)));

        // Dependency
        Operation<?> operation4 = operationsWithDependencyTimes.next();
        assertThat(operation4, equalTo((Operation) operation4Dependency));
        assertThat(operation4.scheduledStartTime(), equalTo(Time.fromMilli(3)));
        assertThat(operation4.dependencyTime(), equalTo(Time.fromMilli(2)));

        // Dependent
        Operation<?> operation5 = operationsWithDependencyTimes.next();
        assertThat(operation5, equalTo((Operation) operation5Dependent));
        assertThat(operation5.scheduledStartTime(), equalTo(Time.fromMilli(4)));
        assertThat(operation5.dependencyTime(), equalTo(Time.fromMilli(3)));

        // Dependent
        Operation<?> operation6 = operationsWithDependencyTimes.next();
        assertThat(operation6, equalTo((Operation) operation6Dependent));
        assertThat(operation6.scheduledStartTime(), equalTo(Time.fromMilli(5)));
        assertThat(operation6.dependencyTime(), equalTo(Time.fromMilli(3)));

        assertThat(operationsWithDependencyTimes.hasNext(), is(false));
    }
}
