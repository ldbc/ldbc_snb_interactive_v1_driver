package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.*;
import com.ldbc.driver.control.*;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TemporalUtil;
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

    TemporalUtil temporalUtil = new TemporalUtil();
    TimeSource timeSource = new SystemTimeSource();

    @Test
    public void shouldGenerateManyElementsInReasonableTime() throws WorkloadException, IOException, DriverConfigurationException {
        // Given
        long MANY_ELEMENTS_COUNT = 1000000;

        Map<String, String> paramsMap = LdbcSnbInteractiveConfiguration.defaultReadOnlyConfig();
        paramsMap.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        // LDBC Interactive Workload-specific parameters
        paramsMap.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        // Driver-specific parameters
        String name = null;
        String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = MANY_ELEMENTS_COUNT;
        int threadCount = 1;
        int statusDisplayIntervalAsSeconds = 0;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();
        double timeCompressionRatio = 1.0;
        Set<String> peerIds = new HashSet<>();
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = false;
        long spinnerSleepDurationAsMilli = 0l;
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
                statusDisplayIntervalAsSeconds,
                timeUnit,
                resultDirPath,
                timeCompressionRatio,
                peerIds,
                validationParams,
                dbValidationFilePath,
                validateWorkload,
                calculateWorkloadStatistics,
                spinnerSleepDurationAsMilli,
                printHelp,
                ignoreScheduledStartTimes,
                shouldCreateResultsLog
        );

        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init(config);

        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        Iterator<Operation<?>> operations = gf.limit(workload.streams(gf).mergeSortedByStartTime(gf), MANY_ELEMENTS_COUNT);
        TimeSource timeSource = new SystemTimeSource();
        long timeoutAsMilli = timeSource.nowAsMilli() + temporalUtil.convert(30, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
        boolean workloadGeneratedOperationsBeforeTimeout = TestUtils.generateBeforeTimeout(operations, timeoutAsMilli, timeSource, MANY_ELEMENTS_COUNT);
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
    public void shouldCreateValidationParametersThenUseThemToPerformDatabaseValidationThenPass() throws ClientException, IOException, DriverConfigurationException {
        // **************************************************
        // where validation parameters should be written (ensure file does not yet exist)
        // **************************************************
        File validationParamsFile = temporaryFolder.newFile();
        assertThat(validationParamsFile.length(), is(0l));

        // **************************************************
        // configuration for generating validation parameters
        // **************************************************
        Map<String, String> paramsMap = LdbcSnbInteractiveConfiguration.defaultReadOnlyConfig();
        paramsMap.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        // DummyDb-specific parameters
        paramsMap.put(DummyLdbcSnbInteractiveDb.SLEEP_DURATION_NANO_ARG, Long.toString(TimeUnit.MILLISECONDS.toNanos(1)));
        // Driver-specific parameters
        String name = "name";
        String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 10000;
        int threadCount = 64;
        int statusDisplayIntervalAsSeconds = 1;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = null;
        double timeCompressionRatio = 1.0;
        Set<String> peerIds = new HashSet<>();
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams =
                new ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions(validationParamsFile.getAbsolutePath(), 1000);
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = false;
        long spinnerSleepDurationAsMilli = 0l;
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
                statusDisplayIntervalAsSeconds,
                timeUnit,
                resultDirPath,
                timeCompressionRatio,
                peerIds,
                validationParams,
                dbValidationFilePath,
                validateWorkload,
                calculateWorkloadStatistics,
                spinnerSleepDurationAsMilli,
                printHelp,
                ignoreScheduledStartTimes,
                shouldCreateResultsLog
        );

        // **************************************************
        // create validation parameters file
        // **************************************************
        Client clientForValidationFileCreation = new Client(new LocalControlService(timeSource.nowAsMilli() + 1000, params), timeSource);
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
                statusDisplayIntervalAsSeconds,
                timeUnit,
                resultDirPath,
                timeCompressionRatio,
                peerIds,
                validationParams,
                dbValidationFilePath,
                validateWorkload,
                calculateWorkloadStatistics,
                spinnerSleepDurationAsMilli,
                printHelp,
                ignoreScheduledStartTimes,
                shouldCreateResultsLog
        );

        // **************************************************
        // validate the database
        // **************************************************
        Client clientForDatabaseValidation = new Client(new LocalControlService(timeSource.nowAsMilli() + 1000, params), timeSource);
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
    public void shouldPassWorkloadValidation() throws ClientException, DriverConfigurationException, IOException {
        // Given
        Map<String, String> paramsMap = LdbcSnbInteractiveConfiguration.defaultReadOnlyConfig();
        paramsMap.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        // DummyDb-specific parameters
        paramsMap.put(DummyLdbcSnbInteractiveDb.SLEEP_DURATION_NANO_ARG, Long.toString(TimeUnit.MILLISECONDS.toNanos(1)));
        // Driver-specific parameters
        String name = null;
        String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 10000;
        int threadCount = 64;
        int statusDisplayIntervalAsSeconds = 1;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = null;
        double timeCompressionRatio = 1.0;
        Set<String> peerIds = new HashSet<>();
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = true;
        boolean calculateWorkloadStatistics = false;
        long spinnerSleepDurationAsMilli = 0l;
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
                statusDisplayIntervalAsSeconds,
                timeUnit,
                resultDirPath,
                timeCompressionRatio,
                peerIds,
                validationParams,
                dbValidationFilePath,
                validateWorkload,
                calculateWorkloadStatistics,
                spinnerSleepDurationAsMilli,
                printHelp,
                ignoreScheduledStartTimes,
                shouldCreateResultsLog
        );

        // When
        Client client = new Client(new LocalControlService(timeSource.nowAsMilli() + 500, params), timeSource);
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
        Map<String, String> paramsMap = LdbcSnbInteractiveConfiguration.defaultReadOnlyConfig();
        paramsMap.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        // LDBC Interactive Workload-specific parameters
        paramsMap.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        // DummyDb-specific parameters
        paramsMap.put(DummyLdbcSnbInteractiveDb.SLEEP_DURATION_NANO_ARG, Long.toString(TimeUnit.MILLISECONDS.toNanos(10)));
        // Driver-specific parameters
        String name = "name";
        String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 10000;
        int threadCount = 1;
        int statusDisplayIntervalAsSeconds = 1;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();
        double timeCompressionRatio = 1.0;
        Set<String> peerIds = new HashSet<>();
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = true;
        long spinnerSleepDurationAsMilli = 10;
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
                statusDisplayIntervalAsSeconds,
                timeUnit,
                resultDirPath,
                timeCompressionRatio,
                peerIds,
                validationParams,
                dbValidationFilePath,
                validateWorkload,
                calculateWorkloadStatistics,
                spinnerSleepDurationAsMilli,
                printHelp,
                ignoreScheduledStartTimes,
                shouldCreateResultsLog
        );

        Workload workloadA = new LdbcSnbInteractiveWorkload();
        workloadA.init(params);

        Workload workloadB = new LdbcSnbInteractiveWorkload();
        workloadB.init(params);

        GeneratorFactory gf1 = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        List<Class> operationsA = ImmutableList.copyOf(
                Iterators.transform(
                        gf1.limit(workloadA.streams(gf1).mergeSortedByStartTime(gf1), params.operationCount()),
                        new Function<Operation<?>, Class>() {
                            @Override
                            public Class apply(Operation<?> operation) {
                                return operation.getClass();
                            }
                        }));

        GeneratorFactory gf2 = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        List<Class> operationsB = ImmutableList.copyOf(
                Iterators.transform(
                        gf2.limit(workloadB.streams(gf2).mergeSortedByStartTime(gf2), params.operationCount()),
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

        workloadA.close();
        workloadB.close();
    }

    @Test
    public void shouldGenerateConfiguredQueryMix() throws ClientException, DriverConfigurationException, WorkloadException, IOException {
        // Given
        String ldbcDriverPropertiesPath = DriverConfigurationFileHelper.getBaseConfigurationFilePublicLocation().getAbsolutePath();
        Properties ldbcDriverProperties = new Properties();
        ldbcDriverProperties.load(new FileInputStream(ldbcDriverPropertiesPath));

        Map<String, String> ldbcDriverParams = ConsoleAndFileDriverConfiguration.convertLongKeysToShortKeys(MapUtils.<String, String>propertiesToMap(ldbcDriverProperties));
        Map<String, String> ldbcSnbParams = LdbcSnbInteractiveConfiguration.defaultReadOnlyConfig();
        ldbcSnbParams.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());

        Map<String, String> baseParams = MapUtils.mergeMaps(ldbcDriverParams, ldbcSnbParams, true);

        Map<String, String> changedQueryMixParams = new HashMap<>();
        changedQueryMixParams.put(ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, "10000");
        changedQueryMixParams.put(ConsoleAndFileDriverConfiguration.DB_ARG, "this will never be used");
        changedQueryMixParams.put(LdbcSnbInteractiveConfiguration.READ_OPERATION_1_FREQUENCY_KEY, "100");
        changedQueryMixParams.put(LdbcSnbInteractiveConfiguration.READ_OPERATION_2_FREQUENCY_KEY, "200");
        changedQueryMixParams.put(LdbcSnbInteractiveConfiguration.READ_OPERATION_3_FREQUENCY_KEY, "400");
        changedQueryMixParams.put(LdbcSnbInteractiveConfiguration.READ_OPERATION_4_FREQUENCY_KEY, "800");
        changedQueryMixParams.put(LdbcSnbInteractiveConfiguration.READ_OPERATION_5_FREQUENCY_KEY, "1600");
        changedQueryMixParams.put(LdbcSnbInteractiveConfiguration.READ_OPERATION_6_FREQUENCY_KEY, "1600");
        changedQueryMixParams.put(LdbcSnbInteractiveConfiguration.READ_OPERATION_7_FREQUENCY_KEY, "800");
        changedQueryMixParams.put(LdbcSnbInteractiveConfiguration.READ_OPERATION_8_FREQUENCY_KEY, "800");
        changedQueryMixParams.put(LdbcSnbInteractiveConfiguration.READ_OPERATION_9_FREQUENCY_KEY, "400");
        changedQueryMixParams.put(LdbcSnbInteractiveConfiguration.READ_OPERATION_10_FREQUENCY_KEY, "200");
        changedQueryMixParams.put(LdbcSnbInteractiveConfiguration.READ_OPERATION_11_FREQUENCY_KEY, "200");
        changedQueryMixParams.put(LdbcSnbInteractiveConfiguration.READ_OPERATION_12_FREQUENCY_KEY, "200");
        changedQueryMixParams.put(LdbcSnbInteractiveConfiguration.READ_OPERATION_13_FREQUENCY_KEY, "100");
        changedQueryMixParams.put(LdbcSnbInteractiveConfiguration.READ_OPERATION_14_FREQUENCY_KEY, "100");
//        changedQueryMixParams.put(LdbcSnbInteractiveWorkload.UPDATE_INTERLEAVE, "1");

        Map<String, String> params = MapUtils.mergeMaps(baseParams, changedQueryMixParams, true);
        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromParamsMap(params);

        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init(configuration);

        // When

        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        Iterator<Class> operationTypes = Iterators.transform(
                gf.limit(workload.streams(gf).mergeSortedByStartTime(gf), configuration.operationCount()),
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

        workload.close();
    }

    @Test
    public void shouldLoadFromConfigFile() throws DriverConfigurationException, ClientException, IOException {
        String ldbcSnbInteractiveTestPropertiesPath =
                new File(DriverConfigurationFileHelper.getWorkloadsDirectory(), "ldbc/snb/interactive/ldbc_snb_interactive.properties").getAbsolutePath();
        String ldbcDriverTestPropertiesPath = DriverConfigurationFileHelper.getBaseConfigurationFilePublicLocation().getAbsolutePath();

        String updateStreamPropertiesPath =
                TestUtils.getResource("/updateStream.properties").getAbsolutePath();

        String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();

        assertThat(new File(resultDirPath).listFiles().length > 0, is(false));

        assertThat(new File(ldbcSnbInteractiveTestPropertiesPath).exists(), is(true));
        assertThat(new File(ldbcDriverTestPropertiesPath).exists(), is(true));

        ConsoleAndFileDriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromArgs(new String[]{
                "-" + ConsoleAndFileDriverConfiguration.RESULT_DIR_PATH_ARG, resultDirPath,
                "-" + ConsoleAndFileDriverConfiguration.DB_ARG, DummyLdbcSnbInteractiveDb.class.getName(),
                "-p", ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, "1000",
                "-p", ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG, "0.00005",
                "-p", LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath(),
                "-P", ldbcSnbInteractiveTestPropertiesPath,
                "-P", ldbcDriverTestPropertiesPath,
                "-P", updateStreamPropertiesPath});

        assertThat(new File(resultDirPath).listFiles().length > 0, is(false));

        // When
        Client client = new Client(new LocalControlService(timeSource.nowAsMilli() + 500, configuration), timeSource);
        client.start();

        // Then
        assertThat(new File(resultDirPath).listFiles().length > 0, is(true));
    }

    @Test
    public void shouldAssignMonotonicallyIncreasingScheduledStartTimesToOperations() throws WorkloadException, IOException, DriverConfigurationException, InterruptedException {
        Map<String, String> paramsMap = LdbcSnbInteractiveConfiguration.defaultReadOnlyConfig();
        paramsMap.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        // Driver-specific parameters
        String name = null;
        String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 1000000;
        int threadCount = 1;
        int statusDisplayIntervalAsSeconds = 1;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();
        double timeCompressionRatio = 1.0;
        Set<String> peerIds = new HashSet<>();
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = true;
        long spinnerSleepDurationAsMilli = 0l;
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
                statusDisplayIntervalAsSeconds,
                timeUnit,
                resultDirPath,
                timeCompressionRatio,
                peerIds,
                validationParams,
                dbValidationFilePath,
                validateWorkload,
                calculateWorkloadStatistics,
                spinnerSleepDurationAsMilli,
                printHelp,
                ignoreScheduledStartTimes,
                shouldCreateResultsLog
        );

        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init(configuration);
        List<Operation<?>> operations = Lists.newArrayList(gf.limit(workload.streams(gf).mergeSortedByStartTime(gf), configuration.operationCount()));

        long prevOperationScheduledStartTimeAsMilli = operations.get(0).scheduledStartTimeAsMilli() - 1;
        for (Operation<?> operation : operations) {
            assertThat(operation.scheduledStartTimeAsMilli() >= prevOperationScheduledStartTimeAsMilli, is(true));
            prevOperationScheduledStartTimeAsMilli = operation.scheduledStartTimeAsMilli();
        }

        workload.close();
    }
}
