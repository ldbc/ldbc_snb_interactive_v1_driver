package com.ldbc.driver.workloads.simple;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.ldbc.driver.*;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.control.DriverConfigurationFileTestHelper;
import com.ldbc.driver.control.LocalControlService;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.testutils.TestUtils;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import com.ldbc.driver.workloads.simple.db.BasicDb;
import org.apache.commons.io.FileUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SimpleWorkloadTest {
    TimeSource TIME_SOURCE = new SystemTimeSource();

    @Test
    public void shouldBeRepeatableWhenSameWorkloadIsUsedTwiceWithIdenticalGeneratorFactories() throws ClientException, DriverConfigurationException, WorkloadException {
        Map<String, String> paramsMap = null;
        String dbClassName = null;
        String workloadClassName = null;
        long operationCount = 100;
        int threadCount = 1;
        Duration statusDisplayInterval = Duration.fromSeconds(0);
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultFilePath = null;
        Double timeCompressionRatio = 1.0;
        Duration windowedExecutionWindowDuration = Duration.fromSeconds(1);
        Set<String> peerIds = new HashSet<>();
        Duration toleratedExecutionDelay = Duration.fromSeconds(1);
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = false;
        Duration spinnerSleepDuration = Duration.fromMilli(0);
        boolean printHelp = false;

        ConsoleAndFileDriverConfiguration params =
                new ConsoleAndFileDriverConfiguration(
                        paramsMap,
                        dbClassName,
                        workloadClassName,
                        operationCount,
                        threadCount,
                        statusDisplayInterval,
                        timeUnit,
                        resultFilePath,
                        timeCompressionRatio,
                        windowedExecutionWindowDuration,
                        peerIds,
                        toleratedExecutionDelay,
                        validationParams,
                        dbValidationFilePath,
                        validateWorkload,
                        calculateWorkloadStatistics,
                        spinnerSleepDuration,
                        printHelp);

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
                        workload.operations(new GeneratorFactory(new RandomDataGeneratorFactory(42L)), params.operationCount()),
                        classFun
                ));

        List<Class> operationsB = ImmutableList.copyOf(
                Iterators.transform(
                        workload.operations(new GeneratorFactory(new RandomDataGeneratorFactory(42L)), params.operationCount()),
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
        workload.cleanup();
    }

    @Ignore
    @Test
    public void shouldPassWorkloadValidation() {
        assertThat(true, is(false));
    }

    @Test
    public void shouldBeRepeatableWhenTwoIdenticalWorkloadsAreUsedWithIdenticalGeneratorFactories() throws ClientException, DriverConfigurationException, WorkloadException {
        Map<String, String> paramsMap = null;
        String dbClassName = null;
        String workloadClassName = null;
        long operationCount = 100;
        int threadCount = 1;
        Duration statusDisplayInterval = Duration.fromSeconds(0);
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultFilePath = null;
        Double timeCompressionRatio = 1.0;
        Duration windowedExecutionWindowDuration = Duration.fromSeconds(1);
        Set<String> peerIds = new HashSet<>();
        Duration toleratedExecutionDelay = Duration.fromSeconds(1);
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = false;
        Duration spinnerSleepDuration = Duration.fromMilli(0);
        boolean printHelp = false;

        ConsoleAndFileDriverConfiguration params =
                new ConsoleAndFileDriverConfiguration(
                        paramsMap,
                        dbClassName,
                        workloadClassName,
                        operationCount,
                        threadCount,
                        statusDisplayInterval,
                        timeUnit,
                        resultFilePath,
                        timeCompressionRatio,
                        windowedExecutionWindowDuration,
                        peerIds,
                        toleratedExecutionDelay,
                        validationParams,
                        dbValidationFilePath,
                        validateWorkload,
                        calculateWorkloadStatistics,
                        spinnerSleepDuration,
                        printHelp);

        Workload workloadA = new SimpleWorkload();
        workloadA.init(params);

        Workload workloadB = new SimpleWorkload();
        workloadB.init(params);

        List<Class> operationsA = ImmutableList.copyOf(
                Iterators.transform(
                        workloadA.operations(new GeneratorFactory(new RandomDataGeneratorFactory(42L)), params.operationCount()),
                        new Function<Operation<?>, Class>() {
                            @Override
                            public Class apply(Operation<?> operation) {
                                return operation.getClass();
                            }
                        }));

        List<Class> operationsB = ImmutableList.copyOf(
                Iterators.transform(
                        workloadB.operations(new GeneratorFactory(new RandomDataGeneratorFactory(42L)), params.operationCount()),
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
                new File(DriverConfigurationFileTestHelper.getWorkloadsDirectory(), "simple/simpleworkload.properties").getAbsolutePath();
        String ldbcDriverTestPropertiesPath =
                TestUtils.getResource("/ldbc_driver_default.properties").getAbsolutePath();

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
        Client client = new Client(new LocalControlService(TIME_SOURCE.now().plus(Duration.fromMilli(500)), configuration), TIME_SOURCE);
        client.start();

        // Then
        assertThat(new File(resultFilePath).exists(), is(true));
        FileUtils.deleteQuietly(new File(resultFilePath));
        assertThat(new File(resultFilePath).exists(), is(false));
    }
}
