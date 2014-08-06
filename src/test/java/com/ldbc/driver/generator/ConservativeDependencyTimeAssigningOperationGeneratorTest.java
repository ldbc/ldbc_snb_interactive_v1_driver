package com.ldbc.driver.generator;

import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.Function1;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation2;
import org.junit.Test;

import java.util.Iterator;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ConservativeDependencyTimeAssigningOperationGeneratorTest {
    @Test
    public void testAssignDependencyTimesEqualToLastEncounteredLowerDependencyStartTime() {
        // Given
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));
        Time initialDependencyTime = Time.fromMilli(0);

        Iterator<Operation<?>> operations = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation1(Time.fromMilli(1), null, null), // R_W
                new TimedNamedOperation2(Time.fromMilli(2), null, null), // R
                new TimedNamedOperation1(Time.fromMilli(3), null, null), // R_W
                new TimedNamedOperation2(Time.fromMilli(3), null, null), // R
                new TimedNamedOperation1(Time.fromMilli(5), null, null), // R_W
                new TimedNamedOperation2(Time.fromMilli(6), null, null), // R
                new TimedNamedOperation1(Time.fromMilli(7), null, null), // R_W
                new TimedNamedOperation2(Time.fromMilli(7), null, null), // R
                new TimedNamedOperation2(Time.fromMilli(8), null, null)   // R
        ).iterator();
        Function1<Operation<?>, Boolean> isDependency = new Function1<Operation<?>, Boolean>() {
            @Override
            public Boolean apply(Operation<?> operation) {
                return operation.getClass().equals(TimedNamedOperation1.class);
            }
        };
        boolean canOverwriteDependencyTime = true;

        // When
        Iterator<Operation<?>> operationsWithDependencyTimes =
                gf.assignDependencyTimesEqualToLastEncounteredLowerDependencyStartTime(operations, isDependency, initialDependencyTime, canOverwriteDependencyTime);

        // Then

        Operation<?> operation1 = operationsWithDependencyTimes.next();
        assertThat(operation1.scheduledStartTime(), equalTo(Time.fromMilli(1)));
        assertThat(operation1.dependencyTime(), equalTo(Time.fromMilli(0)));

        Operation<?> operation2 = operationsWithDependencyTimes.next();
        assertThat(operation2.scheduledStartTime(), equalTo(Time.fromMilli(2)));
        assertThat(operation2.dependencyTime(), equalTo(Time.fromMilli(1)));

        Operation<?> operation3 = operationsWithDependencyTimes.next();
        assertThat(operation3.scheduledStartTime(), equalTo(Time.fromMilli(3)));
        assertThat(operation3.dependencyTime(), equalTo(Time.fromMilli(1)));

        Operation<?> operation4 = operationsWithDependencyTimes.next();
        assertThat(operation4.scheduledStartTime(), equalTo(Time.fromMilli(3)));
        assertThat(operation4.dependencyTime(), equalTo(Time.fromMilli(1)));

        Operation<?> operation5 = operationsWithDependencyTimes.next();
        assertThat(operation5.scheduledStartTime(), equalTo(Time.fromMilli(5)));
        assertThat(operation5.dependencyTime(), equalTo(Time.fromMilli(3)));

        Operation<?> operation6 = operationsWithDependencyTimes.next();
        assertThat(operation6.scheduledStartTime(), equalTo(Time.fromMilli(6)));
        assertThat(operation6.dependencyTime(), equalTo(Time.fromMilli(5)));

        Operation<?> operation7 = operationsWithDependencyTimes.next();
        assertThat(operation7.scheduledStartTime(), equalTo(Time.fromMilli(7)));
        assertThat(operation7.dependencyTime(), equalTo(Time.fromMilli(5)));

        Operation<?> operation8 = operationsWithDependencyTimes.next();
        assertThat(operation8.scheduledStartTime(), equalTo(Time.fromMilli(7)));
        assertThat(operation8.dependencyTime(), equalTo(Time.fromMilli(5)));

        Operation<?> operation9 = operationsWithDependencyTimes.next();
        assertThat(operation9.scheduledStartTime(), equalTo(Time.fromMilli(8)));
        assertThat(operation9.dependencyTime(), equalTo(Time.fromMilli(7)));

        assertThat(operationsWithDependencyTimes.hasNext(), is(false));
    }

    @Test
    public void testAssignDependencyTimesEqualToLastEncounteredDependencyStartTime() {
        // Given
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));
        Time initialDependencyTime = Time.fromMilli(0);

        Iterator<Operation<?>> operations = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation1(Time.fromMilli(1), null, null), // R_W
                new TimedNamedOperation2(Time.fromMilli(2), null, null), // R
                new TimedNamedOperation1(Time.fromMilli(3), null, null), // R_W
                new TimedNamedOperation2(Time.fromMilli(3), null, null), // R
                new TimedNamedOperation1(Time.fromMilli(5), null, null), // R_W
                new TimedNamedOperation2(Time.fromMilli(6), null, null), // R
                new TimedNamedOperation1(Time.fromMilli(7), null, null), // R_W
                new TimedNamedOperation2(Time.fromMilli(7), null, null), // R
                new TimedNamedOperation2(Time.fromMilli(8), null, null)   // R
        ).iterator();
        Function1<Operation<?>, Boolean> isDependency = new Function1<Operation<?>, Boolean>() {
            @Override
            public Boolean apply(Operation<?> operation) {
                return operation.getClass().equals(TimedNamedOperation1.class);
            }
        };
        boolean canOverwriteDependencyTime = true;

        // When
        Iterator<Operation<?>> operationsWithDependencyTimes =
                gf.assignDependencyTimesEqualToLastEncounteredDependencyStartTime(operations, isDependency, initialDependencyTime, canOverwriteDependencyTime);

        // Then

        Operation<?> operation1 = operationsWithDependencyTimes.next();
        assertThat(operation1.scheduledStartTime(), equalTo(Time.fromMilli(1)));
        assertThat(operation1.dependencyTime(), equalTo(Time.fromMilli(0)));

        Operation<?> operation2 = operationsWithDependencyTimes.next();
        assertThat(operation2.scheduledStartTime(), equalTo(Time.fromMilli(2)));
        assertThat(operation2.dependencyTime(), equalTo(Time.fromMilli(1)));

        Operation<?> operation3 = operationsWithDependencyTimes.next();
        assertThat(operation3.scheduledStartTime(), equalTo(Time.fromMilli(3)));
        assertThat(operation3.dependencyTime(), equalTo(Time.fromMilli(1)));

        Operation<?> operation4 = operationsWithDependencyTimes.next();
        assertThat(operation4.scheduledStartTime(), equalTo(Time.fromMilli(3)));
        assertThat(operation4.dependencyTime(), equalTo(Time.fromMilli(3)));

        Operation<?> operation5 = operationsWithDependencyTimes.next();
        assertThat(operation5.scheduledStartTime(), equalTo(Time.fromMilli(5)));
        assertThat(operation5.dependencyTime(), equalTo(Time.fromMilli(3)));

        Operation<?> operation6 = operationsWithDependencyTimes.next();
        assertThat(operation6.scheduledStartTime(), equalTo(Time.fromMilli(6)));
        assertThat(operation6.dependencyTime(), equalTo(Time.fromMilli(5)));

        Operation<?> operation7 = operationsWithDependencyTimes.next();
        assertThat(operation7.scheduledStartTime(), equalTo(Time.fromMilli(7)));
        assertThat(operation7.dependencyTime(), equalTo(Time.fromMilli(5)));

        Operation<?> operation8 = operationsWithDependencyTimes.next();
        assertThat(operation8.scheduledStartTime(), equalTo(Time.fromMilli(7)));
        assertThat(operation8.dependencyTime(), equalTo(Time.fromMilli(7)));

        Operation<?> operation9 = operationsWithDependencyTimes.next();
        assertThat(operation9.scheduledStartTime(), equalTo(Time.fromMilli(8)));
        assertThat(operation9.dependencyTime(), equalTo(Time.fromMilli(7)));

        assertThat(operationsWithDependencyTimes.hasNext(), is(false));
    }

    @Test
    public void testAssignConservativeDependencyTimes() {
        // Given
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));
        Time initialDependencyTime = Time.fromMilli(0);

        Iterator<Operation<?>> operations = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation1(Time.fromMilli(1), null, null), // R_W
                new TimedNamedOperation2(Time.fromMilli(2), null, null), // R
                new TimedNamedOperation1(Time.fromMilli(3), null, null), // R_W
                new TimedNamedOperation2(Time.fromMilli(3), null, null), // R
                new TimedNamedOperation1(Time.fromMilli(5), null, null), // R_W
                new TimedNamedOperation2(Time.fromMilli(6), null, null), // R
                new TimedNamedOperation1(Time.fromMilli(7), null, null), // R_W
                new TimedNamedOperation2(Time.fromMilli(7), null, null), // R
                new TimedNamedOperation2(Time.fromMilli(8), null, null)   // R
        ).iterator();

        boolean canOverwriteDependencyTime = true;

        // When
        Iterator<Operation<?>> operationsWithDependencyTimes =
                gf.assignConservativeDependencyTimes(operations, initialDependencyTime, canOverwriteDependencyTime);

        // Then

        Operation<?> operation1 = operationsWithDependencyTimes.next();
        assertThat(operation1.scheduledStartTime(), equalTo(Time.fromMilli(1)));
        assertThat(operation1.dependencyTime(), equalTo(Time.fromMilli(0)));

        Operation<?> operation2 = operationsWithDependencyTimes.next();
        assertThat(operation2.scheduledStartTime(), equalTo(Time.fromMilli(2)));
        assertThat(operation2.dependencyTime(), equalTo(Time.fromMilli(1)));

        Operation<?> operation3 = operationsWithDependencyTimes.next();
        assertThat(operation3.scheduledStartTime(), equalTo(Time.fromMilli(3)));
        assertThat(operation3.dependencyTime(), equalTo(Time.fromMilli(2)));

        Operation<?> operation4 = operationsWithDependencyTimes.next();
        assertThat(operation4.scheduledStartTime(), equalTo(Time.fromMilli(3)));
        assertThat(operation4.dependencyTime(), equalTo(Time.fromMilli(3)));

        Operation<?> operation5 = operationsWithDependencyTimes.next();
        assertThat(operation5.scheduledStartTime(), equalTo(Time.fromMilli(5)));
        assertThat(operation5.dependencyTime(), equalTo(Time.fromMilli(3)));

        Operation<?> operation6 = operationsWithDependencyTimes.next();
        assertThat(operation6.scheduledStartTime(), equalTo(Time.fromMilli(6)));
        assertThat(operation6.dependencyTime(), equalTo(Time.fromMilli(5)));

        Operation<?> operation7 = operationsWithDependencyTimes.next();
        assertThat(operation7.scheduledStartTime(), equalTo(Time.fromMilli(7)));
        assertThat(operation7.dependencyTime(), equalTo(Time.fromMilli(6)));

        Operation<?> operation8 = operationsWithDependencyTimes.next();
        assertThat(operation8.scheduledStartTime(), equalTo(Time.fromMilli(7)));
        assertThat(operation8.dependencyTime(), equalTo(Time.fromMilli(7)));

        Operation<?> operation9 = operationsWithDependencyTimes.next();
        assertThat(operation9.scheduledStartTime(), equalTo(Time.fromMilli(8)));
        assertThat(operation9.dependencyTime(), equalTo(Time.fromMilli(7)));

        assertThat(operationsWithDependencyTimes.hasNext(), is(false));
    }
}
