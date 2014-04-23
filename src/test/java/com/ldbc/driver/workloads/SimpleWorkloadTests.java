package com.ldbc.driver.workloads;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.ldbc.driver.*;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.control.LocalControlService;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import com.ldbc.driver.workloads.simple.SimpleWorkload;
import com.ldbc.driver.workloads.simple.db.BasicDb;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
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

    @Test
    public void shouldLoadFromConfigFile() throws DriverConfigurationException, ClientException {
        String simpleTestPropertiesPath =
                new File("ldbc_driver/workloads/simple/simpleworkload.properties").getAbsolutePath();
        String ldbcDriverTestPropertiesPath =
                new File("ldbc_driver/src/main/resources/ldbc_driver_default.properties").getAbsolutePath();

        String resultFilePath = "test_write_to_csv_results.json";
        FileUtils.deleteQuietly(new File(resultFilePath));

        assertThat(new File(resultFilePath).exists(), is(false));

        assertThat(new File(simpleTestPropertiesPath).exists(), is(true));
        assertThat(new File(ldbcDriverTestPropertiesPath).exists(), is(true));

        ConsoleAndFileDriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromArgs(new String[]{
                "-" + ConsoleAndFileDriverConfiguration.RESULT_FILE_PATH_ARG, resultFilePath,
                "-" + ConsoleAndFileDriverConfiguration.DB_ARG, BasicDb.class.getName(),
                "-P", simpleTestPropertiesPath,
                "-P", ldbcDriverTestPropertiesPath});


        assertThat(new File(resultFilePath).exists(), is(false));


        // When
        Client client = new Client(new LocalControlService(Time.now().plus(Duration.fromMilli(500)), configuration));
        client.start();

        // Then
        assertThat(new File(resultFilePath).exists(), is(true));
        FileUtils.deleteQuietly(new File(resultFilePath));
        assertThat(new File(resultFilePath).exists(), is(false));
    }}
