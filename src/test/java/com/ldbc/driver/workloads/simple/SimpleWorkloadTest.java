package com.ldbc.driver.workloads.simple;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.ldbc.driver.*;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.control.DriverConfigurationFileHelper;
import com.ldbc.driver.control.LocalControlService;
import com.ldbc.driver.csv.simple.SimpleCsvFileReader;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.metrics.ThreadedQueuedMetricsService;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.testutils.TestUtils;
import com.ldbc.driver.validation.WorkloadValidationResult;
import com.ldbc.driver.workloads.simple.db.BasicDb;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class SimpleWorkloadTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    TimeSource timeSource = new SystemTimeSource();

    @Test
    public void shouldGenerateManyElementsInReasonableTime() throws WorkloadException {
        Map<String, String> paramsMap = null;
        String name = null;
        String dbClassName = null;
        String workloadClassName = null;
        long operationCount = 100;
        int threadCount = 1;
        int statusDisplayInterval = 0;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = null;
        Double timeCompressionRatio = 1.0;
        Set<String> peerIds = new HashSet<>();
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = false;
        long spinnerSleepDuration = 0l;
        boolean printHelp = false;
        boolean ignoreScheduledStartTimes = false;
        boolean shouldCreateResultsLog = false;
        long warmupCount = 100;

        ConsoleAndFileDriverConfiguration params =
                new ConsoleAndFileDriverConfiguration(
                        paramsMap,
                        name,
                        dbClassName,
                        workloadClassName,
                        operationCount,
                        threadCount,
                        statusDisplayInterval,
                        timeUnit,
                        resultDirPath,
                        timeCompressionRatio,
                        peerIds,
                        validationParams,
                        dbValidationFilePath,
                        validateWorkload,
                        calculateWorkloadStatistics,
                        spinnerSleepDuration,
                        printHelp,
                        ignoreScheduledStartTimes,
                        shouldCreateResultsLog,
                        warmupCount
                );

        Workload workload = new SimpleWorkload();
        workload.init(params);
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        Iterator<Operation> operations = gf.limit(
                WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators(gf, workload.streams(gf, true)),
                1_000_000
        );
        TimeSource timeSource = new SystemTimeSource();
        long timeout = timeSource.nowAsMilli() + 30_000l;
        boolean workloadGeneratedOperationsBeforeTimeout = TestUtils.generateBeforeTimeout(operations, timeout, timeSource, 1_000_000);
        assertThat(workloadGeneratedOperationsBeforeTimeout, is(true));
    }

    @Test
    public void shouldBeRepeatableWhenSameWorkloadIsUsedTwiceWithIdenticalGeneratorFactories() throws ClientException, DriverConfigurationException, WorkloadException, IOException {
        Map<String, String> paramsMap = null;
        String name = "name";
        String dbClassName = null;
        String workloadClassName = null;
        long operationCount = 100;
        int threadCount = 1;
        int statusDisplayInterval = 0;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = null;
        Double timeCompressionRatio = 1.0;
        Set<String> peerIds = new HashSet<>();
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = false;
        long spinnerSleepDuration = 0l;
        boolean printHelp = false;
        boolean ignoreScheduledStartTimes = false;
        boolean shouldCreateResultsLog = false;
        long warmupCount = 100;

        ConsoleAndFileDriverConfiguration params =
                new ConsoleAndFileDriverConfiguration(
                        paramsMap,
                        name,
                        dbClassName,
                        workloadClassName,
                        operationCount,
                        threadCount,
                        statusDisplayInterval,
                        timeUnit,
                        resultDirPath,
                        timeCompressionRatio,
                        peerIds,
                        validationParams,
                        dbValidationFilePath,
                        validateWorkload,
                        calculateWorkloadStatistics,
                        spinnerSleepDuration,
                        printHelp,
                        ignoreScheduledStartTimes,
                        shouldCreateResultsLog,
                        warmupCount
                );

        Workload workload = new SimpleWorkload();
        workload.init(params);

        Function<Operation, Class> classFun = new Function<Operation, Class>() {
            @Override
            public Class apply(Operation operation) {
                return operation.getClass();
            }
        };

        GeneratorFactory gf1 = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        List<Class> operationsA = ImmutableList.copyOf(
                Iterators.transform(
                        gf1.limit(
                                WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators(gf1, workload.streams(gf1, true)),
                                params.operationCount()
                        ),
                        classFun
                ));

        GeneratorFactory gf2 = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        List<Class> operationsB = ImmutableList.copyOf(
                Iterators.transform(
                        gf2.limit(
                                WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators(gf2, workload.streams(gf2, true)),
                                params.operationCount()
                        ),
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
        workload.close();
    }

    // TODO operation & operation result serialization/marshalling
    @Ignore
    @Test
    public void shouldPassWorkloadValidation() throws WorkloadException, ClientException {
        // Given
        Map<String, String> paramsMap = new HashMap<>();
        String name = null;
        String dbClassName = BasicDb.class.getName();
        String workloadClassName = SimpleWorkload.class.getName();
        long operationCount = 1000;
        int threadCount = 1;
        int statusDisplayInterval = 1000;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = null;
        Double timeCompressionRatio = 1.0;
        Set<String> peerIds = new HashSet<>();
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = true;
        boolean calculateWorkloadStatistics = false;
        long spinnerSleepDuration = 0l;
        boolean printHelp = false;
        boolean ignoreScheduledStartTimes = false;
        boolean shouldCreateResultsLog = true;
        long warmupCount = 100;

        ConsoleAndFileDriverConfiguration params = new ConsoleAndFileDriverConfiguration(
                paramsMap,
                name,
                dbClassName,
                workloadClassName,
                operationCount,
                threadCount,
                statusDisplayInterval,
                timeUnit,
                resultDirPath,
                timeCompressionRatio,
                peerIds,
                validationParams,
                dbValidationFilePath,
                validateWorkload,
                calculateWorkloadStatistics,
                spinnerSleepDuration,
                printHelp,
                ignoreScheduledStartTimes,
                shouldCreateResultsLog,
                warmupCount
        );

        Workload workload = new SimpleWorkload();
        workload.init(params);

        // When
        Client client = new Client(new LocalControlService(timeSource.nowAsMilli() + 500, params), timeSource);
        client.start();

        // Then
        assertThat(client.databaseValidationResult(), is(nullValue()));
        assertThat(client.workloadStatistics(), is(nullValue()));
        WorkloadValidationResult workloadValidationResult = client.workloadValidationResult();
        assertThat(workloadValidationResult.errorMessage(), workloadValidationResult, is(notNullValue()));
        assertThat(workloadValidationResult.errorMessage(), workloadValidationResult.isSuccessful(), is(true));
    }

    @Test
    public void shouldBeRepeatableWhenTwoIdenticalWorkloadsAreUsedWithIdenticalGeneratorFactories() throws ClientException, DriverConfigurationException, WorkloadException {
        Map<String, String> paramsMap = null;
        String name = "name";
        String dbClassName = null;
        String workloadClassName = null;
        long operationCount = 100;
        int threadCount = 1;
        int statusDisplayInterval = 0;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = null;
        Double timeCompressionRatio = 1.0;
        Set<String> peerIds = new HashSet<>();
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = false;
        long spinnerSleepDuration = 0l;
        boolean printHelp = false;
        boolean ignoreScheduledStartTimes = false;
        boolean shouldCreateResultsLog = false;
        long warmupCount = 100;

        ConsoleAndFileDriverConfiguration params =
                new ConsoleAndFileDriverConfiguration(
                        paramsMap,
                        name,
                        dbClassName,
                        workloadClassName,
                        operationCount,
                        threadCount,
                        statusDisplayInterval,
                        timeUnit,
                        resultDirPath,
                        timeCompressionRatio,
                        peerIds,
                        validationParams,
                        dbValidationFilePath,
                        validateWorkload,
                        calculateWorkloadStatistics,
                        spinnerSleepDuration,
                        printHelp,
                        ignoreScheduledStartTimes,
                        shouldCreateResultsLog,
                        warmupCount
                );

        Workload workloadA = new SimpleWorkload();
        workloadA.init(params);

        Workload workloadB = new SimpleWorkload();
        workloadB.init(params);

        GeneratorFactory gf1 = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        List<Class> operationsA = ImmutableList.copyOf(
                Iterators.transform(
                        gf1.limit(
                                WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators(gf1, workloadA.streams(gf1, true)),
                                params.operationCount()
                        ),
                        new Function<Operation, Class>() {
                            @Override
                            public Class apply(Operation operation) {
                                return operation.getClass();
                            }
                        }));

        GeneratorFactory gf2 = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        List<Class> operationsB = ImmutableList.copyOf(
                Iterators.transform(
                        gf2.limit(
                                WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators(gf2, workloadB.streams(gf2, true)),
                                params.operationCount()
                        ),
                        new Function<Operation, Class>() {
                            @Override
                            public Class apply(Operation operation) {
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
    public void shouldLoadFromConfigFile() throws DriverConfigurationException, ClientException, IOException {
        String simpleTestPropertiesPath =
                new File(DriverConfigurationFileHelper.getWorkloadsDirectory(), "simple/simpleworkload.properties").getAbsolutePath();
        String ldbcDriverTestPropertiesPath = DriverConfigurationFileHelper.getBaseConfigurationFilePublicLocation().getAbsolutePath();

        String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();

        assertThat(new File(resultDirPath).listFiles().length > 0, is(false));

        assertThat(new File(simpleTestPropertiesPath).exists(), is(true));
        assertThat(new File(ldbcDriverTestPropertiesPath).exists(), is(true));

        ConsoleAndFileDriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromArgs(new String[]{
                "-" + ConsoleAndFileDriverConfiguration.RESULT_DIR_PATH_ARG, resultDirPath,
                "-" + ConsoleAndFileDriverConfiguration.DB_ARG, BasicDb.class.getName(),
                "-" + ConsoleAndFileDriverConfiguration.RESULTS_LOG_ARG,
                "-P", simpleTestPropertiesPath,
                "-P", ldbcDriverTestPropertiesPath});


        assertThat(new File(resultDirPath).listFiles().length > 0, is(false));

        // When
        Client client = new Client(new LocalControlService(timeSource.nowAsMilli() + 500, configuration), timeSource);
        client.start();

        // Then
        assertThat(new File(resultDirPath).listFiles().length > 0, is(true));

        File resultsLog = new File(new File(resultDirPath), configuration.name() + ThreadedQueuedMetricsService.RESULTS_LOG_FILENAME_SUFFIX);
        SimpleCsvFileReader csvResultsLogReader = new SimpleCsvFileReader(resultsLog, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING);
        assertThat((long) Iterators.size(csvResultsLogReader), is(configuration.operationCount() + 1)); // + 1 to account for csv headers
        csvResultsLogReader.close();
    }
}
