package com.ldbc.driver.workload;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.ldbc.driver.ClientException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import com.ldbc.driver.workloads.simple.SimpleWorkload;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SimpleWorkloadTests {

    @Test
    public void shouldBeRepeatableWhenSameWorkloadIsUsedTwiceWithIdenticalGeneratorFactories() throws ClientException, DriverConfigurationException, WorkloadException {
        ConsoleAndFileDriverConfiguration params =
                new ConsoleAndFileDriverConfiguration(null, "dbClassName", "workloadClassName", 100L, 1, false, TimeUnit.MILLISECONDS, "resultFilePath", 1.0, Duration.fromSeconds(10), new ArrayList<String>(), Duration.fromSeconds(1));

        Workload workload = new SimpleWorkload();
        workload.init(params);

        Function<Operation<?>, Class> classFun = new Function<Operation<?>, Class>() {
            @Override
            public Class apply(Operation<?> operation) {
                return operation.getClass();
            }
        };

        List<Class> operationsA = ImmutableList.copyOf(
                Iterators.transform(
                        workload.operations(new GeneratorFactory(new RandomDataGeneratorFactory(42L))),
                        classFun
                ));

        List<Class> operationsB = ImmutableList.copyOf(
                Iterators.transform(
                        workload.operations(new GeneratorFactory(new RandomDataGeneratorFactory(42L))),
                        classFun
                ));

        assertThat(operationsA.size(), is(operationsB.size()));

        Iterator<Class> operationsAIt = operationsA.iterator();
        Iterator<Class> operationsBIt = operationsB.iterator();

        while (operationsAIt.hasNext()) {
            Class a = operationsAIt.next();
            Class b = operationsBIt.next();
            assertThat(a, equalTo(b));
        }
    }

    @Test
    public void shouldBeRepeatableWhenTwoIdenticalWorkloadsAreUsedWithIdenticalGeneratorFactories() throws ClientException, DriverConfigurationException, WorkloadException {
        ConsoleAndFileDriverConfiguration params =
                new ConsoleAndFileDriverConfiguration(null, "dbClassName", "workloadClassName", 100L, 1, false, TimeUnit.MILLISECONDS, "resultFilePath", 1.0, Duration.fromSeconds(10), new ArrayList<String>(), Duration.fromSeconds(1));

        Workload workloadA = new SimpleWorkload();
        workloadA.init(params);

        Workload workloadB = new SimpleWorkload();
        workloadB.init(params);

        List<Class> operationsA = ImmutableList.copyOf(
                Iterators.transform(
                        workloadA.operations(new GeneratorFactory(new RandomDataGeneratorFactory(42L))),
                        new Function<Operation<?>, Class>() {
                            @Override
                            public Class apply(Operation<?> operation) {
                                return operation.getClass();
                            }
                        }));

        List<Class> operationsB = ImmutableList.copyOf(
                Iterators.transform(
                        workloadB.operations(new GeneratorFactory(new RandomDataGeneratorFactory(42L))),
                        new Function<Operation<?>, Class>() {
                            @Override
                            public Class apply(Operation<?> operation) {
                                return operation.getClass();
                            }
                        }));

        assertThat(operationsA.size(), is(operationsB.size()));

        Iterator<Class> operationsAIt = operationsA.iterator();
        Iterator<Class> operationsBIt = operationsB.iterator();

        while (operationsAIt.hasNext()) {
            Class a = operationsAIt.next();
            Class b = operationsBIt.next();
            assertThat(a, equalTo(b));
        }
    }
}
