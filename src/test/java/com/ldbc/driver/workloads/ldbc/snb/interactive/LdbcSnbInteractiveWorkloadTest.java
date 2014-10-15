package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.*;
import com.ldbc.driver.control.*;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.runtime.metrics.ThreadedQueuedConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.WorkloadResultsSnapshot;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.testutils.TestUtils;
import com.ldbc.driver.util.Bucket;
import com.ldbc.driver.util.CsvFileReader;
import com.ldbc.driver.util.Histogram;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveDb;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveOperationInstances;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class LdbcSnbInteractiveWorkloadTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    TimeSource timeSource = new SystemTimeSource();

    @Ignore
    @Test
    public void addFunctionalityForSerializingWorkloadStreamWithoutDbImplementationSoItCanBeShared() {
        // TODO functionality would:
        // TODO (1) allow a stream to be written to file, e.g., instead of running
        // TODO (2) allow a stream to be read from file, e.g., instead of generating
        // TODO (3) persist configuration file at the same time, so others can use the same settings
        // TODO (4) read from that configuration file later, so others can use the same settings
        assertThat(true, is(false));
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
        LdbcUpdate1AddPerson write1 = DummyLdbcSnbInteractiveOperationInstances.write1();
        LdbcUpdate2AddPostLike write2 = DummyLdbcSnbInteractiveOperationInstances.write2();
        LdbcUpdate3AddCommentLike write3 = DummyLdbcSnbInteractiveOperationInstances.write3();
        LdbcUpdate4AddForum write4 = DummyLdbcSnbInteractiveOperationInstances.write4();
        LdbcUpdate5AddForumMembership write5 = DummyLdbcSnbInteractiveOperationInstances.write5();
        LdbcUpdate6AddPost write6 = DummyLdbcSnbInteractiveOperationInstances.write6();
        LdbcUpdate7AddComment write7 = DummyLdbcSnbInteractiveOperationInstances.write7();
        LdbcUpdate8AddFriendship write8 = DummyLdbcSnbInteractiveOperationInstances.write8();

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
        String serializedWrite1 = workload.serializeOperation(write1);
        String serializedWrite2 = workload.serializeOperation(write2);
        String serializedWrite3 = workload.serializeOperation(write3);
        String serializedWrite4 = workload.serializeOperation(write4);
        String serializedWrite5 = workload.serializeOperation(write5);
        String serializedWrite6 = workload.serializeOperation(write6);
        String serializedWrite7 = workload.serializeOperation(write7);
        String serializedWrite8 = workload.serializeOperation(write8);

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
        assertThat((Operation) workload.marshalOperation(serializedWrite1), equalTo((Operation) write1));
        assertThat((Operation) workload.marshalOperation(serializedWrite2), equalTo((Operation) write2));
        assertThat((Operation) workload.marshalOperation(serializedWrite3), equalTo((Operation) write3));
        assertThat((Operation) workload.marshalOperation(serializedWrite4), equalTo((Operation) write4));
        assertThat((Operation) workload.marshalOperation(serializedWrite5), equalTo((Operation) write5));
        assertThat((Operation) workload.marshalOperation(serializedWrite6), equalTo((Operation) write6));
        assertThat((Operation) workload.marshalOperation(serializedWrite7), equalTo((Operation) write7));
        assertThat((Operation) workload.marshalOperation(serializedWrite8), equalTo((Operation) write8));
    }

    @Test
    public void shouldGenerateManyElementsInReasonableTime() throws WorkloadException, IOException, DriverConfigurationException {
        // Given
        long MANY_ELEMENTS_COUNT = 1000000;

        Map<String, String> paramsMap = LdbcSnbInteractiveConfiguration.defaultConfig();
        // LDBC Interactive Workload-specific parameters
        paramsMap.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        List<String> forumUpdateFiles = Lists.newArrayList(TestUtils.getResource("/updateStream_0_0_forum.csv").getAbsolutePath());
        paramsMap.put(LdbcSnbInteractiveConfiguration.FORUM_UPDATE_FILES, LdbcSnbInteractiveConfiguration.serializeFilePathsListFromConfiguration(forumUpdateFiles));
        List<String> personUpdateFiles = Lists.newArrayList(TestUtils.getResource("/updateStream_0_0_person.csv").getAbsolutePath());
        paramsMap.put(LdbcSnbInteractiveConfiguration.PERSON_UPDATE_FILES, LdbcSnbInteractiveConfiguration.serializeFilePathsListFromConfiguration(personUpdateFiles));
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
        Duration toleratedExecutionDelay = Duration.fromMinutes(5);
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = false;
        Duration spinnerSleepDuration = Duration.fromMilli(0);
        boolean printHelp = false;
        boolean ignoreScheduledStartTimes = false;
        boolean shouldCreateResultsLog = false;

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

        Map<String, String> updateStreamParams = MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties"));
        configuration = configuration.applyMap(updateStreamParams);

        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init(configuration);

        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        Iterator<Operation<?>> operations = gf.limit(workload.streams(gf).mergeSortedByStartTime(gf), MANY_ELEMENTS_COUNT);
        TimeSource timeSource = new SystemTimeSource();
        Time timeout = timeSource.now().plus(Duration.fromSeconds(30));
        boolean workloadGeneratedOperationsBeforeTimeout = TestUtils.generateBeforeTimeout(operations, timeout, timeSource, MANY_ELEMENTS_COUNT);
        assertThat(workloadGeneratedOperationsBeforeTimeout, is(true));
    }

    @Test
    public void shouldBeRepeatableWhenTwoIdenticalWorkloadsAreUsedWithIdenticalGeneratorFactories() throws ClientException, DriverConfigurationException, WorkloadException, IOException {
        // Given
        Map<String, String> paramsMap = LdbcSnbInteractiveConfiguration.defaultConfig();
        // LDBC Interactive Workload-specific parameters
        paramsMap.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        List<String> forumUpdateFiles = Lists.newArrayList(TestUtils.getResource("/updateStream_0_0_forum.csv").getAbsolutePath());
        paramsMap.put(LdbcSnbInteractiveConfiguration.FORUM_UPDATE_FILES, LdbcSnbInteractiveConfiguration.serializeFilePathsListFromConfiguration(forumUpdateFiles));
        List<String> personUpdateFiles = Lists.newArrayList(TestUtils.getResource("/updateStream_0_0_person.csv").getAbsolutePath());
        paramsMap.put(LdbcSnbInteractiveConfiguration.PERSON_UPDATE_FILES, LdbcSnbInteractiveConfiguration.serializeFilePathsListFromConfiguration(personUpdateFiles));
        // DummyDb-specific parameters
        paramsMap.put(DummyLdbcSnbInteractiveDb.SLEEP_DURATION_MILLI_ARG, Long.toString(Duration.fromMilli(100).asMilli()));
        // Driver-specific parameters
        String name = "name";
        String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 100;
        int threadCount = 1;
        Duration statusDisplayInterval = Duration.fromSeconds(1);
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();
        double timeCompressionRatio = 1.0;
        Duration windowedExecutionWindowDuration = Duration.fromSeconds(1);
        Set<String> peerIds = new HashSet<>();
        Duration toleratedExecutionDelay = Duration.fromMinutes(5);
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

        Map<String, String> updateStreamParams = MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties"));
        configuration = configuration.applyMap(updateStreamParams);

        Workload workloadA = new LdbcSnbInteractiveWorkload();
        workloadA.init(configuration);

        Workload workloadB = new LdbcSnbInteractiveWorkload();
        workloadB.init(configuration);

        GeneratorFactory gf1 = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        List<Class> operationsA = ImmutableList.copyOf(
                Iterators.transform(
                        gf1.limit(workloadA.streams(gf1).mergeSortedByStartTime(gf1), configuration.operationCount()),
                        new Function<Operation<?>, Class>() {
                            @Override
                            public Class apply(Operation<?> operation) {
                                return operation.getClass();
                            }
                        }));

        GeneratorFactory gf2 = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        List<Class> operationsB = ImmutableList.copyOf(
                Iterators.transform(
                        gf1.limit(workloadB.streams(gf2).mergeSortedByStartTime(gf2), configuration.operationCount()),
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
    public void shouldGenerateConfiguredQueryMix() throws ClientException, DriverConfigurationException, WorkloadException {
        // Given
        String ldbcDriverPropertiesPath = TestUtils.getResource("/ldbc_driver_default.properties").getAbsolutePath();

        ConsoleAndFileDriverConfiguration params = ConsoleAndFileDriverConfiguration.fromArgs(new String[]{
                "-w", LdbcSnbInteractiveWorkload.class.getName(),
                "-P", ldbcDriverPropertiesPath,
                // database class is loaded by Client class, which is bypassed in this test
                "-db", "this will never be used",
                "-oc", "10000",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_1_INTERLEAVE_KEY, "100",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_2_INTERLEAVE_KEY, "200",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_3_INTERLEAVE_KEY, "400",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_4_INTERLEAVE_KEY, "800",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_5_INTERLEAVE_KEY, "1600",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_6_INTERLEAVE_KEY, "1600",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_7_INTERLEAVE_KEY, "800",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_8_INTERLEAVE_KEY, "800",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_9_INTERLEAVE_KEY, "400",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_10_INTERLEAVE_KEY, "200",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_11_INTERLEAVE_KEY, "200",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_12_INTERLEAVE_KEY, "200",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_13_INTERLEAVE_KEY, "100",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_14_INTERLEAVE_KEY, "100",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_1_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_2_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_3_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_4_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_5_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_6_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_7_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_8_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_9_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_10_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_11_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_12_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_13_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_14_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_1_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_2_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_3_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_4_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_5_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_6_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_7_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_8_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.FORUM_UPDATE_FILES, TestUtils.getResource("/updateStream_0_0_forum.csv").getAbsolutePath(),
                "-p", LdbcSnbInteractiveConfiguration.PERSON_UPDATE_FILES, TestUtils.getResource("/updateStream_0_0_person.csv").getAbsolutePath(),
                "-p", LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath()
        });

        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init(params);

        // When

        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        Iterator<Class> operationTypes = Iterators.transform(
                gf.limit(workload.streams(gf).mergeSortedByStartTime(gf), params.operationCount()),
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
                TestUtils.getResource("/updateStream.properties").getAbsolutePath();
        String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();

        assertThat(new File(resultDirPath).listFiles().length > 0, is(false));

        assertThat(new File(ldbcSnbInteractiveTestPropertiesPath).exists(), is(true));
        assertThat(new File(ldbcDriverTestPropertiesPath).exists(), is(true));

        ConsoleAndFileDriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromArgs(new String[]{
                "-" + ConsoleAndFileDriverConfiguration.RESULT_DIR_PATH_ARG, resultDirPath,
                "-" + ConsoleAndFileDriverConfiguration.DB_ARG, DummyLdbcSnbInteractiveDb.class.getName(),
                "-p", LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath(),
                "-p", LdbcSnbInteractiveConfiguration.FORUM_UPDATE_FILES, TestUtils.getResource("/updateStream_0_0_forum.csv").getAbsolutePath(),
                "-p", LdbcSnbInteractiveConfiguration.PERSON_UPDATE_FILES, TestUtils.getResource("/updateStream_0_0_person.csv").getAbsolutePath(),
                "-P", ldbcSnbInteractiveTestPropertiesPath,
                "-P", ldbcDriverTestPropertiesPath,
                "-P", updateStreamPropertiesPath});

        // When
        Client client = new Client(new LocalControlService(timeSource.now().plus(Duration.fromMilli(500)), configuration), timeSource);
        client.start();

        // Then
        assertThat(new File(resultDirPath).listFiles().length > 0, is(true));
    }

    @Test
    public void shouldConvertFrequenciesToInterleavesWhenAllFrequenciesProvidedAndAllUpdatesEnabled() throws WorkloadException, DriverConfigurationException {
        // Given
        String ldbcDriverPropertiesPath = TestUtils.getResource("/ldbc_driver_default.properties").getAbsolutePath();

        ConsoleAndFileDriverConfiguration params = ConsoleAndFileDriverConfiguration.fromArgs(new String[]{
                "-w", LdbcSnbInteractiveWorkload.class.getName(),
                "-P", ldbcDriverPropertiesPath,
                // database class is loaded by Client class, which is bypassed in this test
                "-db", "this will never be used",
                "-oc", "100",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_1_FREQUENCY_KEY, "10",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_2_FREQUENCY_KEY, "20",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_3_FREQUENCY_KEY, "30",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_4_FREQUENCY_KEY, "40",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_5_FREQUENCY_KEY, "50",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_6_FREQUENCY_KEY, "60",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_7_FREQUENCY_KEY, "70",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_8_FREQUENCY_KEY, "80",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_9_FREQUENCY_KEY, "90",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_10_FREQUENCY_KEY, "100",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_11_FREQUENCY_KEY, "200",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_12_FREQUENCY_KEY, "300",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_13_FREQUENCY_KEY, "400",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_14_FREQUENCY_KEY, "500",
                "-p", LdbcSnbInteractiveConfiguration.UPDATE_INTERLEAVE, "10",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_1_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_2_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_3_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_4_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_5_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_6_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_7_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_8_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_9_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_10_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_11_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_12_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_13_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_14_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_1_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_2_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_3_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_4_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_5_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_6_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_7_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_8_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.SAFE_T, "1",
                "-p", LdbcSnbInteractiveConfiguration.FORUM_UPDATE_FILES, TestUtils.getResource("/updateStream_0_0_forum.csv").getAbsolutePath(),
                "-p", LdbcSnbInteractiveConfiguration.PERSON_UPDATE_FILES, TestUtils.getResource("/updateStream_0_0_person.csv").getAbsolutePath(),
                "-p", LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath()
        });

        // When
        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init(params);

        // Then
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_1_INTERLEAVE_KEY), equalTo("100"));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_2_INTERLEAVE_KEY), equalTo("200"));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_3_INTERLEAVE_KEY), equalTo("300"));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_4_INTERLEAVE_KEY), equalTo("400"));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_5_INTERLEAVE_KEY), equalTo("500"));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_6_INTERLEAVE_KEY), equalTo("600"));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_7_INTERLEAVE_KEY), equalTo("700"));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_8_INTERLEAVE_KEY), equalTo("800"));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_9_INTERLEAVE_KEY), equalTo("900"));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_10_INTERLEAVE_KEY), equalTo("1000"));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_11_INTERLEAVE_KEY), equalTo("2000"));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_12_INTERLEAVE_KEY), equalTo("3000"));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_13_INTERLEAVE_KEY), equalTo("4000"));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_14_INTERLEAVE_KEY), equalTo("5000"));
    }

    @Test
    public void shouldConvertFrequenciesToInterleavesWhenAllFrequenciesProvidedAndOnlyOneUpdateEnabled() throws WorkloadException, DriverConfigurationException {
        // Given
        String ldbcDriverPropertiesPath = TestUtils.getResource("/ldbc_driver_default.properties").getAbsolutePath();

        ConsoleAndFileDriverConfiguration params = ConsoleAndFileDriverConfiguration.fromArgs(new String[]{
                "-w", LdbcSnbInteractiveWorkload.class.getName(),
                "-P", ldbcDriverPropertiesPath,
                // database class is loaded by Client class, which is bypassed in this test
                "-db", "this will never be used",
                "-oc", "100",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_1_FREQUENCY_KEY, "10",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_2_FREQUENCY_KEY, "20",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_3_FREQUENCY_KEY, "30",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_4_FREQUENCY_KEY, "40",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_5_FREQUENCY_KEY, "50",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_6_FREQUENCY_KEY, "60",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_7_FREQUENCY_KEY, "70",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_8_FREQUENCY_KEY, "80",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_9_FREQUENCY_KEY, "90",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_10_FREQUENCY_KEY, "100",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_11_FREQUENCY_KEY, "200",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_12_FREQUENCY_KEY, "300",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_13_FREQUENCY_KEY, "400",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_14_FREQUENCY_KEY, "500",
                "-p", LdbcSnbInteractiveConfiguration.UPDATE_INTERLEAVE, "10",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_1_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_2_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_3_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_4_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_5_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_6_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_7_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_8_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_9_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_10_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_11_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_12_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_13_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_14_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_1_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_2_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_3_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_4_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_5_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_6_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_7_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_8_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.SAFE_T, "1",
                "-p", LdbcSnbInteractiveConfiguration.FORUM_UPDATE_FILES, TestUtils.getResource("/updateStream_0_0_forum.csv").getAbsolutePath(),
                "-p", LdbcSnbInteractiveConfiguration.PERSON_UPDATE_FILES, TestUtils.getResource("/updateStream_0_0_person.csv").getAbsolutePath(),
                "-p", LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath()
        });

        // When
        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init(params);

        // Then
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_1_INTERLEAVE_KEY), equalTo("100"));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_2_INTERLEAVE_KEY), equalTo("200"));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_3_INTERLEAVE_KEY), equalTo("300"));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_4_INTERLEAVE_KEY), equalTo("400"));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_5_INTERLEAVE_KEY), equalTo("500"));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_6_INTERLEAVE_KEY), equalTo("600"));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_7_INTERLEAVE_KEY), equalTo("700"));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_8_INTERLEAVE_KEY), equalTo("800"));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_9_INTERLEAVE_KEY), equalTo("900"));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_10_INTERLEAVE_KEY), equalTo("1000"));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_11_INTERLEAVE_KEY), equalTo("2000"));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_12_INTERLEAVE_KEY), equalTo("3000"));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_13_INTERLEAVE_KEY), equalTo("4000"));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_14_INTERLEAVE_KEY), equalTo("5000"));
    }

    @Test
    public void shouldConvertFrequenciesToInterleavesWhenAllFrequenciesProvidedAndAllUpdatesDisabled() throws WorkloadException, DriverConfigurationException {
        // Given
        String ldbcDriverPropertiesPath = TestUtils.getResource("/ldbc_driver_default.properties").getAbsolutePath();

        ConsoleAndFileDriverConfiguration params = ConsoleAndFileDriverConfiguration.fromArgs(new String[]{
                "-w", LdbcSnbInteractiveWorkload.class.getName(),
                "-P", ldbcDriverPropertiesPath,
                // database class is loaded by Client class, which is bypassed in this test
                "-db", "this will never be used",
                "-oc", "100",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_1_FREQUENCY_KEY, "10",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_2_FREQUENCY_KEY, "20",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_3_FREQUENCY_KEY, "30",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_4_FREQUENCY_KEY, "40",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_5_FREQUENCY_KEY, "50",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_6_FREQUENCY_KEY, "60",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_7_FREQUENCY_KEY, "70",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_8_FREQUENCY_KEY, "80",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_9_FREQUENCY_KEY, "90",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_10_FREQUENCY_KEY, "100",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_11_FREQUENCY_KEY, "200",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_12_FREQUENCY_KEY, "300",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_13_FREQUENCY_KEY, "400",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_14_FREQUENCY_KEY, "500",
                "-p", LdbcSnbInteractiveConfiguration.UPDATE_INTERLEAVE, "10",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_1_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_2_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_3_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_4_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_5_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_6_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_7_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_8_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_9_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_10_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_11_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_12_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_13_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_14_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_1_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_2_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_3_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_4_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_5_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_6_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_7_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_8_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.SAFE_T, "1",
                "-p", LdbcSnbInteractiveConfiguration.FORUM_UPDATE_FILES, TestUtils.getResource("/updateStream_0_0_forum.csv").getAbsolutePath(),
                "-p", LdbcSnbInteractiveConfiguration.PERSON_UPDATE_FILES, TestUtils.getResource("/updateStream_0_0_person.csv").getAbsolutePath(),
                "-p", LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath()
        });

        // When
        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init(params);

        // Then
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_1_INTERLEAVE_KEY), equalTo(Long.toString(Long.parseLong(LdbcSnbInteractiveConfiguration.DEFAULT_UPDATE_INTERLEAVE) * 10)));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_2_INTERLEAVE_KEY), equalTo(Long.toString(Long.parseLong(LdbcSnbInteractiveConfiguration.DEFAULT_UPDATE_INTERLEAVE) * 20)));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_3_INTERLEAVE_KEY), equalTo(Long.toString(Long.parseLong(LdbcSnbInteractiveConfiguration.DEFAULT_UPDATE_INTERLEAVE) * 30)));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_4_INTERLEAVE_KEY), equalTo(Long.toString(Long.parseLong(LdbcSnbInteractiveConfiguration.DEFAULT_UPDATE_INTERLEAVE) * 40)));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_5_INTERLEAVE_KEY), equalTo(Long.toString(Long.parseLong(LdbcSnbInteractiveConfiguration.DEFAULT_UPDATE_INTERLEAVE) * 50)));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_6_INTERLEAVE_KEY), equalTo(Long.toString(Long.parseLong(LdbcSnbInteractiveConfiguration.DEFAULT_UPDATE_INTERLEAVE) * 60)));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_7_INTERLEAVE_KEY), equalTo(Long.toString(Long.parseLong(LdbcSnbInteractiveConfiguration.DEFAULT_UPDATE_INTERLEAVE) * 70)));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_8_INTERLEAVE_KEY), equalTo(Long.toString(Long.parseLong(LdbcSnbInteractiveConfiguration.DEFAULT_UPDATE_INTERLEAVE) * 80)));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_9_INTERLEAVE_KEY), equalTo(Long.toString(Long.parseLong(LdbcSnbInteractiveConfiguration.DEFAULT_UPDATE_INTERLEAVE) * 90)));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_10_INTERLEAVE_KEY), equalTo(Long.toString(Long.parseLong(LdbcSnbInteractiveConfiguration.DEFAULT_UPDATE_INTERLEAVE) * 100)));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_11_INTERLEAVE_KEY), equalTo(Long.toString(Long.parseLong(LdbcSnbInteractiveConfiguration.DEFAULT_UPDATE_INTERLEAVE) * 200)));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_12_INTERLEAVE_KEY), equalTo(Long.toString(Long.parseLong(LdbcSnbInteractiveConfiguration.DEFAULT_UPDATE_INTERLEAVE) * 300)));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_13_INTERLEAVE_KEY), equalTo(Long.toString(Long.parseLong(LdbcSnbInteractiveConfiguration.DEFAULT_UPDATE_INTERLEAVE) * 400)));
        assertThat(params.asMap().get(LdbcSnbInteractiveConfiguration.READ_OPERATION_14_INTERLEAVE_KEY), equalTo(Long.toString(Long.parseLong(LdbcSnbInteractiveConfiguration.DEFAULT_UPDATE_INTERLEAVE) * 500)));
    }

    @Test
    public void shouldConvertFrequenciesToInterleavesWhenFrequenciesNotProvidedAndAllUpdatesDisabled() throws WorkloadException, DriverConfigurationException {
        // Given
        String ldbcDriverPropertiesPath = TestUtils.getResource("/ldbc_driver_default.properties").getAbsolutePath();

        ConsoleAndFileDriverConfiguration params = ConsoleAndFileDriverConfiguration.fromArgs(new String[]{
                "-w", LdbcSnbInteractiveWorkload.class.getName(),
                "-P", ldbcDriverPropertiesPath,
                // database class is loaded by Client class, which is bypassed in this test
                "-db", "this will never be used",
                "-oc", "100",
                "-p", LdbcSnbInteractiveConfiguration.UPDATE_INTERLEAVE, "10",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_1_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_2_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_3_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_4_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_5_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_6_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_7_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_8_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_9_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_10_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_11_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_12_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_13_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.READ_OPERATION_14_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_1_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_2_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_3_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_4_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_5_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_6_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_7_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_8_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.SAFE_T, "1",
                "-p", LdbcSnbInteractiveConfiguration.FORUM_UPDATE_FILES, TestUtils.getResource("/updateStream_0_0_forum.csv").getAbsolutePath(),
                "-p", LdbcSnbInteractiveConfiguration.PERSON_UPDATE_FILES, TestUtils.getResource("/updateStream_0_0_person.csv").getAbsolutePath(),
                "-p", LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath()
        });

        // When
        boolean exceptionThrown = false;
        Workload workload = new LdbcSnbInteractiveWorkload();
        try {
            workload.init(params);
        } catch (WorkloadException e) {
            exceptionThrown = true;
        }

        // Then
        // either interleaves or frequencies need to be provided
        assertThat(exceptionThrown, is(true));
    }

    @Test
    public void shouldAssignMonotonicallyIncreasingScheduledStartTimesToOperations() throws WorkloadException, IOException, DriverConfigurationException {
        Map<String, String> paramsMap = LdbcSnbInteractiveConfiguration.defaultConfig();
        // LDBC Interactive Workload-specific parameters
        paramsMap.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        List<String> forumUpdateFiles = Lists.newArrayList(TestUtils.getResource("/updateStream_0_0_forum.csv").getAbsolutePath());
        paramsMap.put(LdbcSnbInteractiveConfiguration.FORUM_UPDATE_FILES, LdbcSnbInteractiveConfiguration.serializeFilePathsListFromConfiguration(forumUpdateFiles));
        List<String> personUpdateFiles = Lists.newArrayList(TestUtils.getResource("/updateStream_0_0_person.csv").getAbsolutePath());
        paramsMap.put(LdbcSnbInteractiveConfiguration.PERSON_UPDATE_FILES, LdbcSnbInteractiveConfiguration.serializeFilePathsListFromConfiguration(personUpdateFiles));
        // Driver-specific parameters
        String name = "name";
        String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 1000000;
        int threadCount = 1;
        Duration statusDisplayInterval = Duration.fromSeconds(1);
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();
        double timeCompressionRatio = 0.01;
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

        Map<String, String> updateStreamParams = MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties"));
        configuration = configuration.applyMap(updateStreamParams);

        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init(configuration);
        List<Operation<?>> operations = Lists.newArrayList(gf.limit(workload.streams(gf).mergeSortedByStartTime(gf), configuration.operationCount()));

        Time prevOperationScheduledStartTime = operations.get(0).scheduledStartTime().minus(Duration.fromMilli(1));
        for (Operation<?> operation : operations) {
            assertThat(operation.scheduledStartTime().gte(prevOperationScheduledStartTime), is(true));
            prevOperationScheduledStartTime = operation.scheduledStartTime();
        }

        workload.cleanup();
    }

    @Test
    public void shouldWorkWhenOnlyWriteOperationsAreEnabled() throws WorkloadException, ClientException, IOException, DriverConfigurationException {
        Map<String, String> params = LdbcSnbInteractiveConfiguration.defaultWriteOnlyConfig();
        // LDBC Interactive Workload-specific parameters
        params.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        List<String> forumUpdateFiles = Lists.newArrayList(TestUtils.getResource("/updateStream_0_0_forum.csv").getAbsolutePath());
        params.put(LdbcSnbInteractiveConfiguration.FORUM_UPDATE_FILES, LdbcSnbInteractiveConfiguration.serializeFilePathsListFromConfiguration(forumUpdateFiles));
        List<String> personUpdateFiles = Lists.newArrayList(TestUtils.getResource("/updateStream_0_0_person.csv").getAbsolutePath());
        params.put(LdbcSnbInteractiveConfiguration.PERSON_UPDATE_FILES, LdbcSnbInteractiveConfiguration.serializeFilePathsListFromConfiguration(personUpdateFiles));
        // DummyDb-specific parameters
        params.put(DummyLdbcSnbInteractiveDb.SLEEP_DURATION_MILLI_ARG, Long.toString(Duration.fromMilli(1).asMilli()));
        // Driver-specific parameters
        String name = null;
        String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 500;
        int threadCount = 1;
        Duration statusDisplayInterval = Duration.fromSeconds(1);
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();
        double timeCompressionRatio = 0.00001;
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
        boolean shouldCreateResultsLog = true;

        assertThat(new File(resultDirPath).listFiles().length > 0, is(false));

        DriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(
                params,
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

        Map<String, String> updateStreamParams = MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties"));
        configuration = configuration.applyMap(updateStreamParams);

        Client client = new Client(new LocalControlService(timeSource.now().plus(Duration.fromSeconds(3)), configuration), timeSource);
        client.start();

        assertThat(new File(resultDirPath).listFiles().length > 0, is(true));

        File resultsLog = new File(new File(resultDirPath), configuration.name() + ThreadedQueuedConcurrentMetricsService.RESULTS_LOG_FILENAME_SUFFIX);
        CsvFileReader csvResultsLogReader = new CsvFileReader(resultsLog, CsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        assertThat((long) Iterators.size(csvResultsLogReader), is(configuration.operationCount() + 1)); // + 1 to account for csv headers
    }

    @Test
    public void shouldPassWorkloadValidation() throws ClientException, IOException, DriverConfigurationException {
        Map<String, String> params = LdbcSnbInteractiveConfiguration.defaultWriteOnlyConfig();
        // LDBC Interactive Workload-specific parameters
        params.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        List<String> forumUpdateFiles = Lists.newArrayList(TestUtils.getResource("/updateStream_0_0_forum.csv").getAbsolutePath());
        params.put(LdbcSnbInteractiveConfiguration.FORUM_UPDATE_FILES, LdbcSnbInteractiveConfiguration.serializeFilePathsListFromConfiguration(forumUpdateFiles));
        List<String> personUpdateFiles = Lists.newArrayList(TestUtils.getResource("/updateStream_0_0_person.csv").getAbsolutePath());
        params.put(LdbcSnbInteractiveConfiguration.PERSON_UPDATE_FILES, LdbcSnbInteractiveConfiguration.serializeFilePathsListFromConfiguration(personUpdateFiles));
        // DummyDb-specific parameters
        params.put(DummyLdbcSnbInteractiveDb.SLEEP_DURATION_MILLI_ARG, Long.toString(Duration.fromMilli(1).asMilli()));
        // Driver-specific parameters
        String name = null;
        String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 10000;
        int threadCount = 1;
        Duration statusDisplayInterval = Duration.fromSeconds(1);
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultFolderPath = null;
        double timeCompressionRatio = 0.001;
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

        DriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(
                params,
                name,
                dbClassName,
                workloadClassName,
                operationCount,
                threadCount,
                statusDisplayInterval,
                timeUnit,
                resultFolderPath,
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

        Map<String, String> updateStreamParams = MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties"));
        configuration = configuration.applyMap(updateStreamParams);

        // When
        Client client = new Client(new LocalControlService(timeSource.now().plus(Duration.fromSeconds(3)), configuration), timeSource);
        client.start();

        // Then
        assertThat(client.databaseValidationResult(), is(nullValue()));
        assertThat(client.workloadValidationResult(), is(notNullValue()));
        assertThat(client.workloadValidationResult().errorMessage(), client.workloadValidationResult().isSuccessful(), is(true));
        assertThat(client.workloadStatistics(), is(nullValue()));
    }

    @Ignore
    @Test
    public void performanceTest()
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException, CompletionTimeException, DriverConfigurationException {
        File parentStreamsDir = new File("/Users/alexaverbuch/IdeaProjects/scale_factor_streams/");
        List<File> streamsDirs = Lists.newArrayList(
                new File(parentStreamsDir, "sf_10_partitions_01/"),
                new File(parentStreamsDir, "sf_10_partitions_02/"),
                new File(parentStreamsDir, "sf_10_partitions_04/"),
                new File(parentStreamsDir, "sf_10_partitions_08/"),
                new File(parentStreamsDir, "sf_10_partitions_64/")
        );

        for (File streamDir : streamsDirs) {
            Iterable<String> personFilePaths = Iterables.transform(
                    Lists.newArrayList(
                            streamDir.listFiles(
                                    new FilenameFilter() {
                                        @Override
                                        public boolean accept(File dir, String name) {
                                            return name.endsWith("_person.csv");
                                        }
                                    }
                            )
                    ),
                    new Function<File, String>() {
                        @Override
                        public String apply(File file) {
                            return file.getAbsolutePath();
                        }
                    }
            );
            Iterable<String> forumFilePaths = Iterables.transform(
                    Lists.newArrayList(
                            streamDir.listFiles(
                                    new FilenameFilter() {
                                        @Override
                                        public boolean accept(File dir, String name) {
                                            return name.endsWith("_forum.csv");
                                        }
                                    }
                            )
                    ),
                    new Function<File, String>() {
                        @Override
                        public String apply(File file) {
                            return file.getAbsolutePath();
                        }
                    }
            );

            List<Integer> threadCounts = Lists.newArrayList(1, 2, 4, 8);
            long operationCount = 1000000;
            for (int threadCount : threadCounts) {
                doPerformanceTest(
                        threadCount,
                        operationCount,
                        forumFilePaths,
                        personFilePaths,
                        streamDir.getAbsolutePath(),
                        streamDir.getAbsolutePath(),
                        "TC" + threadCount + "-" + streamDir.getName(),
                        new File(streamDir, "updateStream.properties").getAbsolutePath()
                );
            }
        }
    }

    public void doPerformanceTest(int threadCount,
                                  long operationCount,
                                  Iterable<String> forumFilePaths,
                                  Iterable<String> personFilePaths,
                                  String parametersDir,
                                  String resultsDir,
                                  String name,
                                  String updateStreamPropertiesPath)
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException, CompletionTimeException, DriverConfigurationException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ConcurrentControlService controlService = null;
        Db db = null;
        Workload workload = null;
        ConcurrentMetricsService metricsService = null;
        try {
            Map<String, String> paramsMap = LdbcSnbInteractiveConfiguration.defaultConfig();
            paramsMap.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, parametersDir);
            paramsMap.put(LdbcSnbInteractiveConfiguration.FORUM_UPDATE_FILES, LdbcSnbInteractiveConfiguration.serializeFilePathsListFromConfiguration(forumFilePaths));
            paramsMap.put(LdbcSnbInteractiveConfiguration.PERSON_UPDATE_FILES, LdbcSnbInteractiveConfiguration.serializeFilePathsListFromConfiguration(personFilePaths));
            paramsMap.put(DummyLdbcSnbInteractiveDb.SLEEP_DURATION_MILLI_ARG, "0");
            // Driver-specific parameters
            String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
            String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
            Duration statusDisplayInterval = Duration.fromSeconds(0);
            TimeUnit timeUnit = TimeUnit.MICROSECONDS;
            String resultDirPath = resultsDir;
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
            boolean ignoreScheduledStartTimes = true;
            boolean shouldCreateResultsLog = true;

            ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(
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

            configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(MapUtils.loadPropertiesToMap(new File(updateStreamPropertiesPath)));

            // When
            Client client = new Client(new LocalControlService(timeSource.now().plus(Duration.fromSeconds(3)), configuration), timeSource);
            client.start();

            // Then
            File resultsFile = new File(resultsDir, name + "-results.json");
            WorkloadResultsSnapshot resultsSnapshot = WorkloadResultsSnapshot.fromJson(resultsFile);

            double operationsPerSecond = Math.round(((double) operationCount / resultsSnapshot.totalRunDuration().asNano()) * Duration.fromSeconds(1).asNano());
            double microSecondPerOperation = (double) resultsSnapshot.totalRunDuration().asMicro() / operationCount;
            System.out.println(String.format("[%s]Completed %s operations in %s = %s op/sec = 1 op/%s us", name, operationCount, resultsSnapshot.totalRunDuration(), operationsPerSecond, microSecondPerOperation));
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            if (errorReporter.errorEncountered())
                System.out.println(errorReporter.toString());
            if (null != controlService) controlService.shutdown();
            if (null != db) db.shutdown();
            if (null != workload) workload.cleanup();
            if (null != metricsService) metricsService.shutdown();
        }
    }
}
