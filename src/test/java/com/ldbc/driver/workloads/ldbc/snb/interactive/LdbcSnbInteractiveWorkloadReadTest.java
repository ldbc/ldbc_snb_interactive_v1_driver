package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.*;
import com.ldbc.driver.control.*;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.streams.IteratorSplitter;
import com.ldbc.driver.runtime.streams.IteratorSplittingException;
import com.ldbc.driver.runtime.streams.SplitDefinition;
import com.ldbc.driver.runtime.streams.SplitResult;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.testutils.TestUtils;
import com.ldbc.driver.util.Bucket;
import com.ldbc.driver.util.Histogram;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveDb;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveOperationInstances;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class LdbcSnbInteractiveWorkloadReadTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    TimeSource timeSource = new SystemTimeSource();

    static Map<String, String> defaultSnbParamsMapWithParametersDir() {
        Map<String, String> additionalParams = new HashMap<>();
        additionalParams.put(LdbcSnbInteractiveWorkload.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        return MapUtils.mergeMaps(
                LdbcSnbInteractiveWorkload.defaultReadOnlyConfig(),
                ConsoleAndFileDriverConfiguration.convertLongKeysToShortKeys(additionalParams),
                true);
    }

    @Test
    public void shouldGenerateManyElementsInReasonableTime() throws WorkloadException, IOException {
        // Given
        long MANY_ELEMENTS_COUNT = 1000000;

        Map<String, String> paramsMap = defaultSnbParamsMapWithParametersDir();
        // LDBC Interactive Workload-specific parameters
        paramsMap.put(LdbcSnbInteractiveWorkload.DATA_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        // Driver-specific parameters
        String name = null;
        String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = MANY_ELEMENTS_COUNT;
        int threadCount = 1;
        Duration statusDisplayInterval = Duration.fromSeconds(0);
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();
        double timeCompressionRatio = 1.0;
        Duration windowedExecutionWindowDuration = Duration.fromSeconds(1);
        Set<String> peerIds = new HashSet<>();
        Duration toleratedExecutionDelay = Duration.fromMinutes(60);
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = false;
        Duration spinnerSleepDuration = Duration.fromMilli(0);
        boolean printHelp = false;
        boolean ignoreScheduledStartTimes = false;
        boolean shouldCreateResultsLog = false;

        DriverConfiguration config = new ConsoleAndFileDriverConfiguration(
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
                windowedExecutionWindowDuration,
                peerIds,
                toleratedExecutionDelay,
                validationParams,
                dbValidationFilePath,
                validateWorkload,
                calculateWorkloadStatistics,
                spinnerSleepDuration,
                printHelp,
                ignoreScheduledStartTimes,
                shouldCreateResultsLog
        );

        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init(config);

        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        Iterator<Operation<?>> operations = workload.operations(gf, MANY_ELEMENTS_COUNT);
        TimeSource timeSource = new SystemTimeSource();
        Time timeout = timeSource.now().plus(Duration.fromSeconds(30));
        boolean workloadGeneratedOperationsBeforeTimeout = TestUtils.generateBeforeTimeout(operations, timeout, timeSource, MANY_ELEMENTS_COUNT);
        assertThat(workloadGeneratedOperationsBeforeTimeout, is(true));
    }

    @Test
    public void shouldBeAbleToSerializeAndMarshalAllOperations() throws SerializingMarshallingException {
        // Given
        Workload workload = new LdbcSnbInteractiveWorkload();

        LdbcQuery1 read1 = DummyLdbcSnbInteractiveOperationInstances.read1();
        LdbcQuery2 read2 = DummyLdbcSnbInteractiveOperationInstances.read2();
        LdbcQuery3 read3 = DummyLdbcSnbInteractiveOperationInstances.read3();
        LdbcQuery4 read4 = DummyLdbcSnbInteractiveOperationInstances.read4();
        LdbcQuery5 read5 = DummyLdbcSnbInteractiveOperationInstances.read5();
        LdbcQuery6 read6 = DummyLdbcSnbInteractiveOperationInstances.read6();
        LdbcQuery7 read7 = DummyLdbcSnbInteractiveOperationInstances.read7();
        LdbcQuery8 read8 = DummyLdbcSnbInteractiveOperationInstances.read8();
        LdbcQuery9 read9 = DummyLdbcSnbInteractiveOperationInstances.read9();
        LdbcQuery10 read10 = DummyLdbcSnbInteractiveOperationInstances.read10();
        LdbcQuery11 read11 = DummyLdbcSnbInteractiveOperationInstances.read11();
        LdbcQuery12 read12 = DummyLdbcSnbInteractiveOperationInstances.read12();
        LdbcQuery13 read13 = DummyLdbcSnbInteractiveOperationInstances.read13();
        LdbcQuery14 read14 = DummyLdbcSnbInteractiveOperationInstances.read14();


        // When
        String serializedRead1 = workload.serializeOperation(read1);
        String serializedRead2 = workload.serializeOperation(read2);
        String serializedRead3 = workload.serializeOperation(read3);
        String serializedRead4 = workload.serializeOperation(read4);
        String serializedRead5 = workload.serializeOperation(read5);
        String serializedRead6 = workload.serializeOperation(read6);
        String serializedRead7 = workload.serializeOperation(read7);
        String serializedRead8 = workload.serializeOperation(read8);
        String serializedRead9 = workload.serializeOperation(read9);
        String serializedRead10 = workload.serializeOperation(read10);
        String serializedRead11 = workload.serializeOperation(read11);
        String serializedRead12 = workload.serializeOperation(read12);
        String serializedRead13 = workload.serializeOperation(read13);
        String serializedRead14 = workload.serializeOperation(read14);

        // Then
        assertThat((Operation) workload.marshalOperation(serializedRead1), equalTo((Operation) read1));
        assertThat((Operation) workload.marshalOperation(serializedRead2), equalTo((Operation) read2));
        assertThat((Operation) workload.marshalOperation(serializedRead3), equalTo((Operation) read3));
        assertThat((Operation) workload.marshalOperation(serializedRead4), equalTo((Operation) read4));
        assertThat((Operation) workload.marshalOperation(serializedRead5), equalTo((Operation) read5));
        assertThat((Operation) workload.marshalOperation(serializedRead6), equalTo((Operation) read6));
        assertThat((Operation) workload.marshalOperation(serializedRead7), equalTo((Operation) read7));
        assertThat((Operation) workload.marshalOperation(serializedRead8), equalTo((Operation) read8));
        assertThat((Operation) workload.marshalOperation(serializedRead9), equalTo((Operation) read9));
        assertThat((Operation) workload.marshalOperation(serializedRead10), equalTo((Operation) read10));
        assertThat((Operation) workload.marshalOperation(serializedRead11), equalTo((Operation) read11));
        assertThat((Operation) workload.marshalOperation(serializedRead12), equalTo((Operation) read12));
        assertThat((Operation) workload.marshalOperation(serializedRead13), equalTo((Operation) read13));
        assertThat((Operation) workload.marshalOperation(serializedRead14), equalTo((Operation) read14));
    }

    @Test
    public void testConfigFileShouldContainSameParamsAndValuesAsTestParamsInThisTestClass() throws IOException {
        // Given
        String ldbcSnbInteractiveReadTestParamsFromFilePath = TestUtils.getResource("/ldbc_snb_interactive_read_test.properties").getAbsolutePath();
        Properties ldbcSnbInteractiveReadTestParamsFromFileProperties = new Properties();

        String ldbcSnbInteractiveUpdateStream = TestUtils.getResource("/updateStream_0.properties").getAbsolutePath();
        Properties ldbcSnbInteractiveUpdateStreamProperties = new Properties();
        ldbcSnbInteractiveUpdateStreamProperties.load(new FileInputStream(ldbcSnbInteractiveUpdateStream));

        Map<String, String> ldbcSnbInteractiveParamsFromUpdateStream =
                ConsoleAndFileDriverConfiguration.convertLongKeysToShortKeys(MapUtils.<String, String>propertiesToMap(ldbcSnbInteractiveUpdateStreamProperties));

        Map<String, String> ldbcSnbInteractiveUpdateDistance = new HashMap<String, String>();
        ldbcSnbInteractiveUpdateDistance.put(LdbcSnbInteractiveWorkload.UPDATE_INTERLEAVE,
                ldbcSnbInteractiveParamsFromUpdateStream.get(LdbcSnbInteractiveWorkload.UPDATE_INTERLEAVE));

        ldbcSnbInteractiveReadTestParamsFromFileProperties.load(new FileInputStream(ldbcSnbInteractiveReadTestParamsFromFilePath));
        Map<String, String> ldbcSnbInteractiveReadTestParamsFromFile =
                ConsoleAndFileDriverConfiguration.convertLongKeysToShortKeys(MapUtils.<String, String>propertiesToMap(ldbcSnbInteractiveReadTestParamsFromFileProperties));

        Map<String, String> unionLdbcSnbInteractiveReadTestParamsFromFiles =
                MapUtils.mergeMaps(ldbcSnbInteractiveUpdateDistance, ldbcSnbInteractiveReadTestParamsFromFile,true);

        Map<String, String> ldbcSnbInteractiveReadTestParams = LdbcSnbInteractiveWorkload.defaultReadOnlyConfig();

        // When

        // Then
        assertThat(
                String.format("Expected:\n%sFound:\n%s",
                        MapUtils.prettyPrint(unionLdbcSnbInteractiveReadTestParamsFromFiles, "\t"),
                        MapUtils.prettyPrint(ldbcSnbInteractiveReadTestParams, "\t")),
                unionLdbcSnbInteractiveReadTestParamsFromFiles,
                equalTo(ldbcSnbInteractiveReadTestParams));
    }

    @Test
    public void shouldCreateValidationParametersThenUseThemToPerformDatabaseValidationThenPass() throws ClientException, IOException {
        // **************************************************
        // where validation parameters should be written (ensure file does not yet exist)
        // **************************************************
        File validationParamsFile = temporaryFolder.newFile();
        assertThat(validationParamsFile.length(), is(0l));

        // **************************************************
        // configuration for generating validation parameters
        // **************************************************
        Map<String, String> paramsMap = defaultSnbParamsMapWithParametersDir();
        // DummyDb-specific parameters
        paramsMap.put(DummyLdbcSnbInteractiveDb.SLEEP_DURATION_MILLI_ARG, Long.toString(Duration.fromMilli(1).asMilli()));
        // Driver-specific parameters
        String name = "name";
        String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 10000;
        int threadCount = 64;
        Duration statusDisplayInterval = Duration.fromSeconds(1);
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = null;
        double timeCompressionRatio = 1.0;
        Duration windowedExecutionWindowDuration = Duration.fromSeconds(1);
        Set<String> peerIds = new HashSet<>();
        Duration toleratedExecutionDelay = Duration.fromMinutes(60);
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams =
                new ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions(validationParamsFile.getAbsolutePath(), 1000);
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = false;
        Duration spinnerSleepDuration = Duration.fromMilli(0);
        boolean printHelp = false;
        boolean ignoreScheduledStartTimes = false;
        boolean shouldCreateResultsLog = false;

        DriverConfiguration params = new ConsoleAndFileDriverConfiguration(
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
                windowedExecutionWindowDuration,
                peerIds,
                toleratedExecutionDelay,
                validationParams,
                dbValidationFilePath,
                validateWorkload,
                calculateWorkloadStatistics,
                spinnerSleepDuration,
                printHelp,
                ignoreScheduledStartTimes,
                shouldCreateResultsLog
        );

        // **************************************************
        // create validation parameters file
        // **************************************************
        Client clientForValidationFileCreation = new Client(new LocalControlService(timeSource.now().plus(Duration.fromMilli(1000)), params), timeSource);
        clientForValidationFileCreation.start();

        // **************************************************
        // check that validation file creation worked
        // **************************************************
        assertThat(validationParamsFile.length() > 0, is(true));
        assertThat(clientForValidationFileCreation.workloadValidationResult(), is(nullValue()));
        assertThat(clientForValidationFileCreation.workloadStatistics(), is(nullValue()));
        assertThat(clientForValidationFileCreation.databaseValidationResult(), is(nullValue()));

        // **************************************************
        // configuration for using validation parameters file to validate the database
        // **************************************************
        validationParams = null;
        dbValidationFilePath = validationParamsFile.getAbsolutePath();

        params = new ConsoleAndFileDriverConfiguration(
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
                windowedExecutionWindowDuration,
                peerIds,
                toleratedExecutionDelay,
                validationParams,
                dbValidationFilePath,
                validateWorkload,
                calculateWorkloadStatistics,
                spinnerSleepDuration,
                printHelp,
                ignoreScheduledStartTimes,
                shouldCreateResultsLog
        );

        // **************************************************
        // validate the database
        // **************************************************
        Client clientForDatabaseValidation = new Client(new LocalControlService(timeSource.now().plus(Duration.fromMilli(1000)), params), timeSource);
        clientForDatabaseValidation.start();

        // **************************************************
        // check that validation was successful
        // **************************************************
        assertThat(validationParamsFile.length() > 0, is(true));
        assertThat(clientForDatabaseValidation.workloadValidationResult(), is(nullValue()));
        assertThat(clientForDatabaseValidation.workloadStatistics(), is(nullValue()));
        assertThat(clientForDatabaseValidation.databaseValidationResult(), is(notNullValue()));
        assertThat(
                String.format("Validation with following error\n%s", clientForDatabaseValidation.databaseValidationResult().resultMessage()),
                clientForDatabaseValidation.databaseValidationResult().isSuccessful(),
                is(true));
    }

    @Test
    public void shouldPassWorkloadValidation() throws ClientException {
        // Given
        Map<String, String> paramsMap = defaultSnbParamsMapWithParametersDir();
        // DummyDb-specific parameters
        paramsMap.put(DummyLdbcSnbInteractiveDb.SLEEP_DURATION_MILLI_ARG, Long.toString(Duration.fromMilli(1).asMilli()));
        // Driver-specific parameters
        String name = null;
        String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 10000;
        int threadCount = 64;
        Duration statusDisplayInterval = Duration.fromSeconds(1);
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = null;
        double timeCompressionRatio = 1.0;
        Duration windowedExecutionWindowDuration = Duration.fromSeconds(1);
        Set<String> peerIds = new HashSet<>();
        Duration toleratedExecutionDelay = Duration.fromMinutes(60);
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = true;
        boolean calculateWorkloadStatistics = false;
        Duration spinnerSleepDuration = Duration.fromMilli(0);
        boolean printHelp = false;
        boolean ignoreScheduledStartTimes = false;
        boolean shouldCreateResultsLog = false;

        DriverConfiguration params = new ConsoleAndFileDriverConfiguration(
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
                windowedExecutionWindowDuration,
                peerIds,
                toleratedExecutionDelay,
                validationParams,
                dbValidationFilePath,
                validateWorkload,
                calculateWorkloadStatistics,
                spinnerSleepDuration,
                printHelp,
                ignoreScheduledStartTimes,
                shouldCreateResultsLog
        );

        // When
        Client client = new Client(new LocalControlService(timeSource.now().plus(Duration.fromMilli(500)), params), timeSource);
        client.start();

        // Then
        assertThat(client.databaseValidationResult(), is(nullValue()));
        assertThat(client.workloadValidationResult(), is(notNullValue()));
        assertThat(client.workloadValidationResult().isSuccessful(), is(true));
        assertThat(client.workloadStatistics(), is(nullValue()));
    }

    @Test
    public void shouldBeRepeatableWhenTwoIdenticalWorkloadsAreUsedWithIdenticalGeneratorFactories() throws ClientException, DriverConfigurationException, WorkloadException, IOException {
        // Given
        Map<String, String> paramsMap = defaultSnbParamsMapWithParametersDir();
        // LDBC Interactive Workload-specific parameters
        paramsMap.put(LdbcSnbInteractiveWorkload.DATA_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        // DummyDb-specific parameters
        paramsMap.put(DummyLdbcSnbInteractiveDb.SLEEP_DURATION_MILLI_ARG, Long.toString(Duration.fromMilli(10).asMilli()));
        // Driver-specific parameters
        String name = "name";
        String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 10000;
        int threadCount = 1;
        Duration statusDisplayInterval = Duration.fromSeconds(1);
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();
        double timeCompressionRatio = 1.0;
        Duration windowedExecutionWindowDuration = Duration.fromSeconds(1);
        Set<String> peerIds = new HashSet<>();
        Duration toleratedExecutionDelay = Duration.fromMinutes(60);
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = true;
        Duration spinnerSleepDuration = Duration.fromMilli(10);
        boolean printHelp = false;
        boolean ignoreScheduledStartTimes = false;
        boolean shouldCreateResultsLog = false;

        assertThat(new File(resultDirPath).listFiles().length > 0, is(false));

        DriverConfiguration params = new ConsoleAndFileDriverConfiguration(
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
                windowedExecutionWindowDuration,
                peerIds,
                toleratedExecutionDelay,
                validationParams,
                dbValidationFilePath,
                validateWorkload,
                calculateWorkloadStatistics,
                spinnerSleepDuration,
                printHelp,
                ignoreScheduledStartTimes,
                shouldCreateResultsLog
        );

        Workload workloadA = new LdbcSnbInteractiveWorkload();
        workloadA.init(params);

        Workload workloadB = new LdbcSnbInteractiveWorkload();
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

        workloadA.cleanup();
        workloadB.cleanup();
    }

    @Test
    public void shouldGenerateConfiguredQueryMix() throws ClientException, DriverConfigurationException, WorkloadException, IOException {
        // Given
        String ldbcDriverPropertiesPath = TestUtils.getResource("/ldbc_driver_default.properties").getAbsolutePath();
        Properties ldbcDriverProperties = new Properties();
        ldbcDriverProperties.load(new FileInputStream(ldbcDriverPropertiesPath));

        Map<String, String> ldbcDriverParams = ConsoleAndFileDriverConfiguration.convertLongKeysToShortKeys(MapUtils.<String, String>propertiesToMap(ldbcDriverProperties));

        Map<String, String> baseParams = MapUtils.mergeMaps(ldbcDriverParams, defaultSnbParamsMapWithParametersDir(), true);

        Map<String, String> changedQueryMixParams = new HashMap<>();
        changedQueryMixParams.put(ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, "10000");
        changedQueryMixParams.put(ConsoleAndFileDriverConfiguration.DB_ARG, "this will never be used");
        changedQueryMixParams.put(LdbcSnbInteractiveWorkload.READ_OPERATION_1_FREQUENCY_KEY, "100");
        changedQueryMixParams.put(LdbcSnbInteractiveWorkload.READ_OPERATION_2_FREQUENCY_KEY, "200");
        changedQueryMixParams.put(LdbcSnbInteractiveWorkload.READ_OPERATION_3_FREQUENCY_KEY, "400");
        changedQueryMixParams.put(LdbcSnbInteractiveWorkload.READ_OPERATION_4_FREQUENCY_KEY, "800");
        changedQueryMixParams.put(LdbcSnbInteractiveWorkload.READ_OPERATION_5_FREQUENCY_KEY, "1600");
        changedQueryMixParams.put(LdbcSnbInteractiveWorkload.READ_OPERATION_6_FREQUENCY_KEY, "1600");
        changedQueryMixParams.put(LdbcSnbInteractiveWorkload.READ_OPERATION_7_FREQUENCY_KEY, "800");
        changedQueryMixParams.put(LdbcSnbInteractiveWorkload.READ_OPERATION_8_FREQUENCY_KEY, "800");
        changedQueryMixParams.put(LdbcSnbInteractiveWorkload.READ_OPERATION_9_FREQUENCY_KEY, "400");
        changedQueryMixParams.put(LdbcSnbInteractiveWorkload.READ_OPERATION_10_FREQUENCY_KEY, "200");
        changedQueryMixParams.put(LdbcSnbInteractiveWorkload.READ_OPERATION_11_FREQUENCY_KEY, "200");
        changedQueryMixParams.put(LdbcSnbInteractiveWorkload.READ_OPERATION_12_FREQUENCY_KEY, "200");
        changedQueryMixParams.put(LdbcSnbInteractiveWorkload.READ_OPERATION_13_FREQUENCY_KEY, "100");
        changedQueryMixParams.put(LdbcSnbInteractiveWorkload.READ_OPERATION_14_FREQUENCY_KEY, "100");
        changedQueryMixParams.put(LdbcSnbInteractiveWorkload.UPDATE_INTERLEAVE, "1");

        Map<String, String> params = MapUtils.mergeMaps(baseParams, changedQueryMixParams, true);
        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromParamsMap(params);

        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init(configuration);

        // When

        Iterator<Class> operationTypes = Iterators.transform(
                workload.operations(new GeneratorFactory(new RandomDataGeneratorFactory(42L)), configuration.operationCount()),
                new Function<Operation<?>, Class>() {
                    @Override
                    public Class apply(Operation<?> operation) {
                        return operation.getClass();
                    }
                });

        // Then
        Histogram<Class, Double> expectedQueryMixHistogram = new Histogram<>(0d);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery1.class), 1d / 100);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery2.class), 1d / 200);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery3.class), 1d / 400);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery4.class), 1d / 800);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery5.class), 1d / 1600);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery6.class), 1d / 1600);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery7.class), 1d / 800);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery8.class), 1d / 800);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery9.class), 1d / 400);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery10.class), 1d / 200);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery11.class), 1d / 200);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery12.class), 1d / 200);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery13.class), 1d / 100);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery14.class), 1d / 100);

        Histogram<Class, Long> actualQueryMixHistogram = new Histogram<>(0l);
        actualQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery1.class), 0l);
        actualQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery2.class), 0l);
        actualQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery3.class), 0l);
        actualQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery4.class), 0l);
        actualQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery5.class), 0l);
        actualQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery6.class), 0l);
        actualQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery7.class), 0l);
        actualQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery8.class), 0l);
        actualQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery9.class), 0l);
        actualQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery10.class), 0l);
        actualQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery11.class), 0l);
        actualQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery12.class), 0l);
        actualQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery13.class), 0l);
        actualQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery14.class), 0l);
        actualQueryMixHistogram.importValueSequence(operationTypes);

        double tolerance = 0.01d;

        assertThat(
                String.format("Distributions should be within tolerance: %s\n%s\n%s",
                        tolerance,
                        actualQueryMixHistogram.toPercentageValues().toPrettyString(),
                        expectedQueryMixHistogram.toPercentageValues().toPrettyString()),
                Histogram.equalsWithinTolerance(
                        actualQueryMixHistogram.toPercentageValues(),
                        expectedQueryMixHistogram.toPercentageValues(),
                        tolerance),
                is(true));

        workload.cleanup();
    }

    @Test
    public void shouldLoadFromConfigFile() throws DriverConfigurationException, ClientException, IOException {
        String ldbcSnbInteractiveTestPropertiesPath =
                new File(DriverConfigurationFileTestHelper.getWorkloadsDirectory(), "ldbc/snb/interactive/ldbc_snb_interactive.properties").getAbsolutePath();
        String ldbcDriverTestPropertiesPath =
                TestUtils.getResource("/ldbc_driver_default.properties").getAbsolutePath();

        String updateStreamPropertiesPath =
                TestUtils.getResource("/updateStream_0.properties").getAbsolutePath();

        String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();

        assertThat(new File(resultDirPath).listFiles().length > 0, is(false));

        assertThat(new File(ldbcSnbInteractiveTestPropertiesPath).exists(), is(true));
        assertThat(new File(ldbcDriverTestPropertiesPath).exists(), is(true));

        ConsoleAndFileDriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromArgs(new String[]{
                "-" + ConsoleAndFileDriverConfiguration.RESULT_DIR_PATH_ARG, resultDirPath,
                "-" + ConsoleAndFileDriverConfiguration.DB_ARG, DummyLdbcSnbInteractiveDb.class.getName(),
                "-p", LdbcSnbInteractiveWorkload.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath(),
                "-P", ldbcSnbInteractiveTestPropertiesPath,
                "-P", ldbcDriverTestPropertiesPath,
                "-P", updateStreamPropertiesPath});

        assertThat(new File(resultDirPath).listFiles().length > 0, is(false));

        // When
        Client client = new Client(new LocalControlService(timeSource.now().plus(Duration.fromMilli(500)), configuration), timeSource);
        client.start();

        // Then
        assertThat(new File(resultDirPath).listFiles().length > 0, is(true));
    }

    @Test
    public void shouldAssignMonotonicallyIncreasingScheduledStartTimesToOperations() throws WorkloadException, IOException, DriverConfigurationException, InterruptedException {
        Map<String, String> paramsMap = defaultSnbParamsMapWithParametersDir();
        // Driver-specific parameters
        String name = null;
        String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 1000000;
        int threadCount = 1;
        Duration statusDisplayInterval = Duration.fromSeconds(1);
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();
        double timeCompressionRatio = 1.0;
        Duration windowedExecutionWindowDuration = Duration.fromSeconds(1);
        Set<String> peerIds = new HashSet<>();
        Duration toleratedExecutionDelay = Duration.fromSeconds(1);
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = true;
        Duration spinnerSleepDuration = Duration.fromMilli(0);
        boolean printHelp = false;
        boolean ignoreScheduledStartTimes = false;
        boolean shouldCreateResultsLog = false;

        assertThat(new File(resultDirPath).listFiles().length > 0, is(false));

        DriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(
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
                windowedExecutionWindowDuration,
                peerIds,
                toleratedExecutionDelay,
                validationParams,
                dbValidationFilePath,
                validateWorkload,
                calculateWorkloadStatistics,
                spinnerSleepDuration,
                printHelp,
                ignoreScheduledStartTimes,
                shouldCreateResultsLog
        );

        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init(configuration);
        List<Operation<?>> operations = Lists.newArrayList(workload.operations(new GeneratorFactory(new RandomDataGeneratorFactory(42L)), configuration.operationCount()));

        Time prevOperationScheduledStartTime = operations.get(0).scheduledStartTime().minus(Duration.fromMilli(1));
        for (Operation<?> operation : operations) {
            assertThat(operation.scheduledStartTime().gte(prevOperationScheduledStartTime), is(true));
            prevOperationScheduledStartTime = operation.scheduledStartTime();
        }

        workload.cleanup();
    }

    @Test
    public void operationsShouldHaveMonotonicallyIncreasingScheduledStartTimesAfterSplitting()
            throws WorkloadException, IOException, DriverConfigurationException, IteratorSplittingException {
        Map<String, String> paramsMap = defaultSnbParamsMapWithParametersDir();
        // Driver-specific parameters
        String name = "name";
        String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 1000000;
        int threadCount = 1;
        Duration statusDisplayInterval = Duration.fromSeconds(1);
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();
        double timeCompressionRatio = 1.0;
        Duration windowedExecutionWindowDuration = Duration.fromSeconds(1);
        Set<String> peerIds = new HashSet<>();
        Duration toleratedExecutionDelay = Duration.fromSeconds(1);
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = true;
        Duration spinnerSleepDuration = Duration.fromMilli(0);
        boolean printHelp = false;
        boolean ignoreScheduledStartTimes = false;
        boolean shouldCreateResultsLog = false;

        assertThat(new File(resultDirPath).listFiles().length > 0, is(false));

        DriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(
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
                windowedExecutionWindowDuration,
                peerIds,
                toleratedExecutionDelay,
                validationParams,
                dbValidationFilePath,
                validateWorkload,
                calculateWorkloadStatistics,
                spinnerSleepDuration,
                printHelp,
                ignoreScheduledStartTimes,
                shouldCreateResultsLog
        );

        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init(configuration);
        List<Operation<?>> operations = Lists.newArrayList(workload.operations(new GeneratorFactory(new RandomDataGeneratorFactory(42L)), configuration.operationCount()));

        Time firstOperationScheduledStartTime = operations.get(0).scheduledStartTime();

        Time prevOperationScheduledStartTime = firstOperationScheduledStartTime.minus(Duration.fromMilli(1));
        for (Operation<?> operation : Lists.newArrayList(operations)) {
            assertThat(operation.scheduledStartTime().gte(prevOperationScheduledStartTime), is(true));
            prevOperationScheduledStartTime = operation.scheduledStartTime();
        }

        IteratorSplitter<Operation<?>> splitter = new IteratorSplitter<>(IteratorSplitter.UnmappedItemPolicy.ABORT);
        SplitDefinition<Operation<?>> windowed = new SplitDefinition<>(Workload.operationTypesBySchedulingMode(workload.operationClassifications(), OperationClassification.SchedulingMode.WINDOWED));
        SplitDefinition<Operation<?>> blocking = new SplitDefinition<>(Workload.operationTypesBySchedulingMode(workload.operationClassifications(), OperationClassification.SchedulingMode.INDIVIDUAL_BLOCKING));
        SplitDefinition<Operation<?>> asynchronous = new SplitDefinition<>(Workload.operationTypesBySchedulingMode(workload.operationClassifications(), OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC));
        SplitResult splits = splitter.split(operations.iterator(), windowed, blocking, asynchronous);
        List<Operation<?>> windowedOperations = Lists.newArrayList(splits.getSplitFor(windowed));
        List<Operation<?>> blockingOperations = Lists.newArrayList(splits.getSplitFor(blocking));
        List<Operation<?>> asynchronousOperations = Lists.newArrayList(splits.getSplitFor(asynchronous));

        Time prevWindowedOperationScheduledStartTime = firstOperationScheduledStartTime.minus(Duration.fromMilli(1));
        for (Operation<?> operation : windowedOperations) {
            assertThat(operation.scheduledStartTime().gte(prevWindowedOperationScheduledStartTime), is(true));
            prevWindowedOperationScheduledStartTime = operation.scheduledStartTime();
        }

        Time prevAsyncOperationScheduledStartTime = firstOperationScheduledStartTime.minus(Duration.fromMilli(1));
        for (Operation<?> operation : asynchronousOperations) {
            assertThat(operation.scheduledStartTime().gte(prevAsyncOperationScheduledStartTime), is(true));
            prevAsyncOperationScheduledStartTime = operation.scheduledStartTime();
        }

        Time prevBlockingOperationScheduledStartTime = firstOperationScheduledStartTime.minus(Duration.fromMilli(1));
        for (Operation<?> operation : blockingOperations) {
            assertThat(operation.scheduledStartTime().gte(prevBlockingOperationScheduledStartTime), is(true));
            prevBlockingOperationScheduledStartTime = operation.scheduledStartTime();
        }

        workload.cleanup();
    }
}
