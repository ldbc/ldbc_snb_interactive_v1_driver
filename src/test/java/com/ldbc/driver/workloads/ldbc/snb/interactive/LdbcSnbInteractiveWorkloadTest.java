package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.*;
import com.ldbc.driver.control.*;
import com.ldbc.driver.csv.simple.SimpleCsvFileReader;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.metrics.ThreadedQueuedMetricsService;
import com.ldbc.driver.temporal.SystemTimeSource;
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
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

public class LdbcSnbInteractiveWorkloadTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    TimeSource timeSource = new SystemTimeSource();

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

        LdbcShortQuery1PersonProfile shortRead1 = DummyLdbcSnbInteractiveOperationInstances.short1();
        LdbcShortQuery2PersonPosts shortRead2 = DummyLdbcSnbInteractiveOperationInstances.short2();
        LdbcShortQuery3PersonFriends shortRead3 = DummyLdbcSnbInteractiveOperationInstances.short3();
        LdbcShortQuery4MessageContent shortRead4 = DummyLdbcSnbInteractiveOperationInstances.short4();
        LdbcShortQuery5MessageCreator shortRead5 = DummyLdbcSnbInteractiveOperationInstances.short5();
        LdbcShortQuery6MessageForum shortRead6 = DummyLdbcSnbInteractiveOperationInstances.short6();
        LdbcShortQuery7MessageReplies shortRead7 = DummyLdbcSnbInteractiveOperationInstances.short7();

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

        String serializedShortRead1 = workload.serializeOperation(shortRead1);
        String serializedShortRead2 = workload.serializeOperation(shortRead2);
        String serializedShortRead3 = workload.serializeOperation(shortRead3);
        String serializedShortRead4 = workload.serializeOperation(shortRead4);
        String serializedShortRead5 = workload.serializeOperation(shortRead5);
        String serializedShortRead6 = workload.serializeOperation(shortRead6);
        String serializedShortRead7 = workload.serializeOperation(shortRead7);

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

        assertThat((Operation) workload.marshalOperation(serializedShortRead1), equalTo((Operation) shortRead1));
        assertThat((Operation) workload.marshalOperation(serializedShortRead2), equalTo((Operation) shortRead2));
        assertThat((Operation) workload.marshalOperation(serializedShortRead3), equalTo((Operation) shortRead3));
        assertThat((Operation) workload.marshalOperation(serializedShortRead4), equalTo((Operation) shortRead4));
        assertThat((Operation) workload.marshalOperation(serializedShortRead5), equalTo((Operation) shortRead5));
        assertThat((Operation) workload.marshalOperation(serializedShortRead6), equalTo((Operation) shortRead6));
        assertThat((Operation) workload.marshalOperation(serializedShortRead7), equalTo((Operation) shortRead7));

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
    public void shouldGenerateManyOperationsInReasonableTimeForLongReadOnly() throws WorkloadException, IOException, DriverConfigurationException {
        Map<String, String> workloadParams = LdbcSnbInteractiveConfiguration.withoutWrites(
                LdbcSnbInteractiveConfiguration.withoutShortReads(
                        LdbcSnbInteractiveConfiguration.defaultConfigSF1()
                )
        );
        workloadParams.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams = MapUtils.mergeMaps(
                workloadParams,
                MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties")),
                true
        );
        doShouldGenerateManyElementsInReasonableTime(workloadParams, 1_000_000, TimeUnit.SECONDS.toMillis(5));
    }

    @Test
    public void shouldGenerateManyOperationsInReasonableTimeForWriteOnly() throws WorkloadException, IOException, DriverConfigurationException {
        Map<String, String> workloadParams = LdbcSnbInteractiveConfiguration.withoutLongReads(
                LdbcSnbInteractiveConfiguration.withoutShortReads(
                        LdbcSnbInteractiveConfiguration.defaultConfigSF1()
                )
        );
        workloadParams.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams = MapUtils.mergeMaps(
                workloadParams,
                MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties")),
                true
        );
        doShouldGenerateManyElementsInReasonableTime(workloadParams, 50_000, TimeUnit.SECONDS.toMillis(5));
    }

    @Test
    public void shouldGenerateManyOperationsInReasonableTimeForReadOnly() throws WorkloadException, IOException, DriverConfigurationException {
        Map<String, String> workloadParams = LdbcSnbInteractiveConfiguration.withoutWrites(
                LdbcSnbInteractiveConfiguration.defaultConfigSF1()
        );
        workloadParams.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams = MapUtils.mergeMaps(
                workloadParams,
                MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties")),
                true
        );
        doShouldGenerateManyElementsInReasonableTime(workloadParams, 1_000_000, TimeUnit.SECONDS.toMillis(5));
    }

    @Test
    public void shouldGenerateManyOperationsInReasonableTimeForFullWorkload() throws WorkloadException, IOException, DriverConfigurationException {
        Map<String, String> workloadParams = LdbcSnbInteractiveConfiguration.defaultConfigSF1();
        workloadParams.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams = MapUtils.mergeMaps(
                workloadParams,
                MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties")),
                true
        );
        doShouldGenerateManyElementsInReasonableTime(workloadParams, 1_000_000, TimeUnit.SECONDS.toMillis(5));
    }

    @Test
    public void shouldGenerateManyOperationsInReasonableTimeForOperation1Only() throws WorkloadException, IOException, DriverConfigurationException {
        Map<String, String> workloadParams = LdbcSnbInteractiveConfiguration.defaultConfigSF1();
        workloadParams.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams = MapUtils.mergeMaps(
                workloadParams,
                MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties")),
                true
        );
        doShouldGenerateManyElementsInReasonableTime(workloadParams, 1_000_000, TimeUnit.SECONDS.toMillis(5));
    }

    private void doShouldGenerateManyElementsInReasonableTime(Map<String, String> workloadParams, long operationCount, long timeoutAsMilli) throws WorkloadException, IOException, DriverConfigurationException {
        // Given

        // Driver-specific parameters
        String name = null;
        String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        int threadCount = 1;
        int statusDisplayInterval = 0;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();
        double timeCompressionRatio = 1.0;
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

        DriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(
                workloadParams,
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

        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init(configuration);

        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        Iterator<Operation> operations = gf.limit(WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators(gf, workload.streams(gf, true)), operationCount);
        TimeSource timeSource = new SystemTimeSource();
        long timeout = timeSource.nowAsMilli() + timeoutAsMilli;
        boolean workloadGeneratedOperationsBeforeTimeout = TestUtils.generateBeforeTimeout(operations, timeout, timeSource, operationCount);
        assertThat(workloadGeneratedOperationsBeforeTimeout, is(true));
    }

    @Test
    public void shouldBeRepeatableWhenTwoIdenticalWorkloadsAreUsedWithIdenticalGeneratorFactories() throws ClientException, DriverConfigurationException, WorkloadException, IOException {
        // Given
        Map<String, String> paramsMap = LdbcSnbInteractiveConfiguration.defaultConfigSF1();
        // LDBC Interactive Workload-specific parameters
        paramsMap.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        paramsMap.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        // DummyDb-specific parameters
        paramsMap.put(DummyLdbcSnbInteractiveDb.SLEEP_DURATION_NANO_ARG, Long.toString(TimeUnit.MILLISECONDS.toNanos(100)));
        paramsMap.put(DummyLdbcSnbInteractiveDb.SLEEP_TYPE_ARG, DummyLdbcSnbInteractiveDb.SleepType.SPIN.name());
        // Driver-specific parameters
        String name = "name";
        String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 100;
        int threadCount = 1;
        int statusDisplayInterval = 1;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();
        double timeCompressionRatio = 1.0;
        Set<String> peerIds = new HashSet<>();
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = true;
        long spinnerSleepDuration = 0l;
        boolean printHelp = false;
        boolean ignoreScheduledStartTimes = false;
        boolean shouldCreateResultsLog = false;
        long warmupCount = 100;

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

        Map<String, String> updateStreamParams = MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties"));
        configuration = configuration.applyArgs(updateStreamParams);

        Workload workloadA = new LdbcSnbInteractiveWorkload();
        workloadA.init(configuration);

        Workload workloadB = new LdbcSnbInteractiveWorkload();
        workloadB.init(configuration);

        GeneratorFactory gf1 = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        List<Class> operationsA = ImmutableList.copyOf(
                Iterators.transform(
                        gf1.limit(WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators(gf1, workloadA.streams(gf1, true)), configuration.operationCount()),
                        new Function<Operation, Class>() {
                            @Override
                            public Class apply(Operation operation) {
                                return operation.getClass();
                            }
                        }));

        GeneratorFactory gf2 = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        List<Class> operationsB = ImmutableList.copyOf(
                Iterators.transform(
                        gf1.limit(WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators(gf2, workloadB.streams(gf2, true)), configuration.operationCount()),
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

        workloadA.close();
        workloadB.close();
    }

    @Test
    public void shouldGenerateConfiguredQueryMix() throws ClientException, DriverConfigurationException, WorkloadException, IOException {
        // Given
        String ldbcDriverPropertiesPath = DriverConfigurationFileHelper.getBaseConfigurationFilePublicLocation().getAbsolutePath();

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
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_1_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_2_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_3_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_4_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_5_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_6_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_7_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_8_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_9_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_10_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_11_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_12_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_13_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_14_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_1_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_2_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_3_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_4_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_5_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_6_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_7_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_8_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath(),
                "-p", LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath()
        });

        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init(params);

        // When

        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        Iterator<Class> operationTypes = Iterators.transform(
                gf.limit(WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators(gf, workload.streams(gf, true)), params.operationCount()),
                new Function<Operation, Class>() {
                    @Override
                    public Class apply(Operation operation) {
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

        String ldbcSnbInteractiveTestPropertiesPath = LdbcSnbInteractiveConfiguration.defaultConfigFileSF1().getAbsolutePath();
        String ldbcDriverTestPropertiesPath = DriverConfigurationFileHelper.getBaseConfigurationFilePublicLocation().getAbsolutePath();
        String updateStreamPropertiesPath = TestUtils.getResource("/updateStream.properties").getAbsolutePath();
        String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();

        assertThat(new File(resultDirPath).listFiles().length > 0, is(false));

        assertThat(new File(ldbcSnbInteractiveTestPropertiesPath).exists(), is(true));
        assertThat(new File(ldbcDriverTestPropertiesPath).exists(), is(true));

        ConsoleAndFileDriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromArgs(new String[]{
                "-" + ConsoleAndFileDriverConfiguration.RESULT_DIR_PATH_ARG, resultDirPath,
                "-" + ConsoleAndFileDriverConfiguration.DB_ARG, DummyLdbcSnbInteractiveDb.class.getName(),
                "-" + ConsoleAndFileDriverConfiguration.TIME_UNIT_ARG, TimeUnit.MICROSECONDS.name(),
                "-p", ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, Long.toString(10_000),
                "-p", ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG, Double.toString(0.00001),
                "-p", LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath(),
                "-p", LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath(),
                "-P", ldbcSnbInteractiveTestPropertiesPath,
                "-P", ldbcDriverTestPropertiesPath,
                "-P", updateStreamPropertiesPath});

        // When
        Client client = new Client(new LocalControlService(timeSource.nowAsMilli() + 500, configuration), timeSource);
        client.start();

        // Then
        assertThat(new File(resultDirPath).listFiles().length > 0, is(true));
    }

    @Test
    public void shouldConvertFrequenciesToInterleavesWhenAllFrequenciesProvidedAndAllUpdatesEnabled() throws WorkloadException, DriverConfigurationException {
        // Given
        String ldbcDriverPropertiesPath = DriverConfigurationFileHelper.getBaseConfigurationFilePublicLocation().getAbsolutePath();

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
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_1_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_2_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_3_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_4_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_5_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_6_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_7_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_8_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_9_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_10_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_11_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_12_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_13_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_14_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_1_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_2_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_3_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_4_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_5_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_6_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_7_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_8_ENABLE_KEY, "true",
                // TOOD remove
//                "-p", LdbcSnbInteractiveConfiguration.SAFE_T, "1",
                "-p", LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath(),
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
        String ldbcDriverPropertiesPath = DriverConfigurationFileHelper.getBaseConfigurationFilePublicLocation().getAbsolutePath();

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
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_1_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_2_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_3_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_4_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_5_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_6_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_7_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_8_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_9_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_10_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_11_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_12_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_13_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_14_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_1_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_2_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_3_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_4_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_5_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_6_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_7_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_8_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath(),
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
        String ldbcDriverPropertiesPath = DriverConfigurationFileHelper.getBaseConfigurationFilePublicLocation().getAbsolutePath();

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
                "-p", LdbcSnbInteractiveConfiguration.UPDATE_INTERLEAVE, "1",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_1_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_2_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_3_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_4_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_5_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_6_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_7_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_8_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_9_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_10_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_11_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_12_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_13_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_14_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.SHORT_READ_OPERATION_1_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.SHORT_READ_OPERATION_2_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.SHORT_READ_OPERATION_3_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.SHORT_READ_OPERATION_4_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.SHORT_READ_OPERATION_5_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.SHORT_READ_OPERATION_6_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.SHORT_READ_OPERATION_7_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_1_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_2_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_3_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_4_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_5_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_6_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_7_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_8_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath(),
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
        String ldbcDriverPropertiesPath = DriverConfigurationFileHelper.getBaseConfigurationFilePublicLocation().getAbsolutePath();

        ConsoleAndFileDriverConfiguration params = ConsoleAndFileDriverConfiguration.fromArgs(new String[]{
                "-w", LdbcSnbInteractiveWorkload.class.getName(),
                "-P", ldbcDriverPropertiesPath,
                // database class is loaded by Client class, which is bypassed in this test
                "-db", "this will never be used",
                "-oc", "100",
                "-p", LdbcSnbInteractiveConfiguration.UPDATE_INTERLEAVE, "10",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_1_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_2_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_3_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_4_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_5_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_6_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_7_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_8_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_9_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_10_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_11_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_12_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_13_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.LONG_READ_OPERATION_14_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_1_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_2_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_3_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_4_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_5_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_6_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_7_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.WRITE_OPERATION_8_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath(),
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
        Map<String, String> paramsMap = LdbcSnbInteractiveConfiguration.defaultConfigSF1();
        // LDBC Interactive Workload-specific parameters
        paramsMap.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        paramsMap.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        // Driver-specific parameters
        String name = "name";
        String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 1000000;
        int threadCount = 1;
        int statusDisplayInterval = 1;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();
        double timeCompressionRatio = 0.01;
        Set<String> peerIds = new HashSet<>();
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = true;
        long spinnerSleepDuration = 0l;
        boolean printHelp = false;
        boolean ignoreScheduledStartTimes = false;
        boolean shouldCreateResultsLog = false;
        long warmupCount = 100;

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

        Map<String, String> updateStreamParams = MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties"));
        configuration = configuration.applyArgs(updateStreamParams);

        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init(configuration);
        List<Operation> operations = Lists.newArrayList(
                gf.limit(WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators(gf, workload.streams(gf, true)), configuration.operationCount())
        );

        long prevOperationScheduledStartTime = operations.get(0).scheduledStartTimeAsMilli() - 1;
        for (Operation operation : operations) {
            assertThat(operation.scheduledStartTimeAsMilli() >= prevOperationScheduledStartTime, is(true));
            prevOperationScheduledStartTime = operation.scheduledStartTimeAsMilli();
        }

        workload.close();
    }

    @Test
    public void shouldRunWorkloadForLongReadsOnly() throws WorkloadException, ClientException, IOException, DriverConfigurationException {
        Map<String, String> workloadParams = LdbcSnbInteractiveConfiguration.withoutWrites(
                LdbcSnbInteractiveConfiguration.withoutShortReads(
                        LdbcSnbInteractiveConfiguration.defaultConfigSF1()
                )
        );
        workloadParams.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams = MapUtils.mergeMaps(
                workloadParams,
                MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties")),
                true
        );
        doShouldRunWorkload(workloadParams, 1_000, 100_000);
    }

    @Test
    public void shouldRunWorkloadForReadsOnly() throws WorkloadException, ClientException, IOException, DriverConfigurationException {
        Map<String, String> workloadParams = LdbcSnbInteractiveConfiguration.withoutWrites(
                LdbcSnbInteractiveConfiguration.defaultConfigSF1()
        );
        workloadParams.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams = MapUtils.mergeMaps(
                workloadParams,
                MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties")),
                true
        );
        doShouldRunWorkload(workloadParams, 100_000, 10_000);
    }

    @Test
    public void shouldRunWorkloadForWritesOnly() throws WorkloadException, ClientException, IOException, DriverConfigurationException {
        Map<String, String> workloadParams = LdbcSnbInteractiveConfiguration.withoutLongReads(
                LdbcSnbInteractiveConfiguration.withoutShortReads(
                        LdbcSnbInteractiveConfiguration.defaultConfigSF1()
                )
        );
        workloadParams.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams = MapUtils.mergeMaps(
                workloadParams,
                MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties")),
                true
        );
        doShouldRunWorkload(workloadParams, 1, 50_000);
    }

    @Test
    public void shouldRunWorkloadForFullWorkload() throws WorkloadException, ClientException, IOException, DriverConfigurationException {
        Map<String, String> workloadParams = LdbcSnbInteractiveConfiguration.defaultConfigSF1();
        workloadParams.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams = MapUtils.mergeMaps(
                workloadParams,
                MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties")),
                true
        );
        doShouldRunWorkload(workloadParams, 0, 100_000);
    }

    private void doShouldRunWorkload(Map<String, String> workloadParams, long warmupCount, long operationCount) throws WorkloadException, ClientException, IOException, DriverConfigurationException {
        // DummyDb-specific parameters
        workloadParams.put(DummyLdbcSnbInteractiveDb.SLEEP_DURATION_NANO_ARG, Long.toString(TimeUnit.MICROSECONDS.toNanos(10)));
        workloadParams.put(DummyLdbcSnbInteractiveDb.SLEEP_TYPE_ARG, DummyLdbcSnbInteractiveDb.SleepType.SPIN.name());
        // Driver-specific parameters
        String name = null;
        String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        int threadCount = 1;
        int statusDisplayInterval = 1;
        TimeUnit timeUnit = TimeUnit.MICROSECONDS;
        String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();
        double timeCompressionRatio = 0.0000001;
        Set<String> peerIds = new HashSet<>();
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = false;
        long spinnerSleepDuration = 0l;
        boolean printHelp = false;
        boolean ignoreScheduledStartTimes = false;
        boolean shouldCreateResultsLog = true;

        assertThat(new File(resultDirPath).listFiles().length > 0, is(false));

        DriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(
                workloadParams,
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

        Client client = new Client(new LocalControlService(timeSource.nowAsMilli() + 3000, configuration), timeSource);
        client.start();

        int expectedFileCount = (0 == warmupCount)
                ? 3
                : 6;
        assertThat(new File(resultDirPath).listFiles().length, is(expectedFileCount));

        File resultsLog = new File(new File(resultDirPath), configuration.name() + ThreadedQueuedMetricsService.RESULTS_LOG_FILENAME_SUFFIX);
        SimpleCsvFileReader csvResultsLogReader = new SimpleCsvFileReader(resultsLog, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING);
        assertThat((long) Iterators.size(csvResultsLogReader), allOf(greaterThanOrEqualTo(percent(operationCount, 0.95)), lessThanOrEqualTo(percent(operationCount, 1.05))));
    }

    @Test
    public void shouldPassWorkloadValidationForLongReadsOnly() throws WorkloadException, ClientException, IOException, DriverConfigurationException {
        Map<String, String> workloadParams = LdbcSnbInteractiveConfiguration.withoutWrites(
                LdbcSnbInteractiveConfiguration.withoutShortReads(
                        LdbcSnbInteractiveConfiguration.defaultConfigSF1()
                )
        );
        workloadParams.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams = MapUtils.mergeMaps(
                workloadParams,
                MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties")),
                true
        );
        doShouldPassWorkloadValidation(workloadParams, 100_000);
    }

    @Test
    public void shouldPassWorkloadValidationForReadsOnly() throws WorkloadException, ClientException, IOException, DriverConfigurationException {
        Map<String, String> workloadParams = LdbcSnbInteractiveConfiguration.withoutWrites(
                LdbcSnbInteractiveConfiguration.defaultConfigSF1()
        );
        workloadParams.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams = MapUtils.mergeMaps(
                workloadParams,
                MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties")),
                true
        );
        doShouldPassWorkloadValidation(workloadParams, 100_000);
    }

    @Test
    public void shouldPassWorkloadValidationForWritesOnly() throws WorkloadException, ClientException, IOException, DriverConfigurationException {
        Map<String, String> workloadParams = LdbcSnbInteractiveConfiguration.withoutLongReads(
                LdbcSnbInteractiveConfiguration.withoutShortReads(
                        LdbcSnbInteractiveConfiguration.defaultConfigSF1()
                )
        );
        workloadParams.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams = MapUtils.mergeMaps(
                workloadParams,
                MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties")),
                true
        );
        doShouldPassWorkloadValidation(workloadParams, 100_000);
    }

    @Test
    public void shouldPassWorkloadValidationForFullWorkload() throws WorkloadException, ClientException, IOException, DriverConfigurationException {
        Map<String, String> workloadParams = LdbcSnbInteractiveConfiguration.defaultConfigSF1();
        workloadParams.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams = MapUtils.mergeMaps(
                workloadParams,
                MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties")),
                true
        );
        doShouldPassWorkloadValidation(workloadParams, 100_000);
    }

    private void doShouldPassWorkloadValidation(Map<String, String> workloadParams, long operationCount) throws ClientException, IOException, DriverConfigurationException {
        // DummyDb-specific parameters
        workloadParams.put(DummyLdbcSnbInteractiveDb.SLEEP_DURATION_NANO_ARG, Long.toString(TimeUnit.MILLISECONDS.toNanos(1)));
        workloadParams.put(DummyLdbcSnbInteractiveDb.SLEEP_TYPE_ARG, DummyLdbcSnbInteractiveDb.SleepType.SPIN.name());
        // Driver-specific parameters
        String name = null;
        String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        int threadCount = 1;
        int statusDisplayInterval = 1;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultFolderPath = null;
        double timeCompressionRatio = 0.001;
        Set<String> peerIds = new HashSet<>();
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = true;
        boolean calculateWorkloadStatistics = false;
        long spinnerSleepDuration = 0l;
        boolean printHelp = false;
        boolean ignoreScheduledStartTimes = false;
        boolean shouldCreateResultsLog = false;
        long warmupCount = 100;

        DriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(
                workloadParams,
                name,
                dbClassName,
                workloadClassName,
                operationCount,
                threadCount,
                statusDisplayInterval,
                timeUnit,
                resultFolderPath,
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

        Map<String, String> updateStreamParams = MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties"));
        configuration = configuration.applyArgs(updateStreamParams);

        // When
        Client client = new Client(new LocalControlService(timeSource.nowAsMilli() + 3000, configuration), timeSource);
        client.start();

        // Then
        assertThat(client.databaseValidationResult(), is(nullValue()));
        assertThat(client.workloadValidationResult(), is(notNullValue()));
        assertThat(client.workloadValidationResult().errorMessage(), client.workloadValidationResult().isSuccessful(), is(true));
        assertThat(client.workloadStatistics(), is(nullValue()));
    }

    @Test
    public void shouldCreateValidationParametersThenUseThemToPerformDatabaseValidationThenPassForLongReadsOnly() throws ClientException, IOException, DriverConfigurationException {
        Map<String, String> workloadParams = LdbcSnbInteractiveConfiguration.withoutShortReads(
                LdbcSnbInteractiveConfiguration.withoutWrites(
                        LdbcSnbInteractiveConfiguration.defaultConfigSF1()
                )
        );
        workloadParams.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams = MapUtils.mergeMaps(
                workloadParams,
                MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties")),
                true
        );
        doShouldCreateValidationParametersThenUseThemToPerformDatabaseValidationThenPass(workloadParams);
    }

    @Test
    public void shouldCreateValidationParametersThenUseThemToPerformDatabaseValidationThenPassForReadsOnly() throws ClientException, IOException, DriverConfigurationException {
        Map<String, String> workloadParams = LdbcSnbInteractiveConfiguration.withoutWrites(
                LdbcSnbInteractiveConfiguration.defaultConfigSF1()
        );
        workloadParams.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams = MapUtils.mergeMaps(
                workloadParams,
                MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties")),
                true
        );
        doShouldCreateValidationParametersThenUseThemToPerformDatabaseValidationThenPass(workloadParams);
    }

    @Test
    public void shouldCreateValidationParametersThenUseThemToPerformDatabaseValidationThenPassForWritesOnly() throws ClientException, IOException, DriverConfigurationException {
        Map<String, String> workloadParams = LdbcSnbInteractiveConfiguration.withoutLongReads(
                LdbcSnbInteractiveConfiguration.withoutShortReads(
                        LdbcSnbInteractiveConfiguration.defaultConfigSF1()
                )
        );
        workloadParams.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams = MapUtils.mergeMaps(
                workloadParams,
                MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties")),
                true
        );
        doShouldCreateValidationParametersThenUseThemToPerformDatabaseValidationThenPass(workloadParams);
    }

    @Test
    public void shouldCreateValidationParametersThenUseThemToPerformDatabaseValidationThenPassForFullWorkload() throws ClientException, IOException, DriverConfigurationException {
        Map<String, String> workloadParams = LdbcSnbInteractiveConfiguration.defaultConfigSF1();
        workloadParams.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        workloadParams = MapUtils.mergeMaps(
                workloadParams,
                MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties")),
                true
        );
        doShouldCreateValidationParametersThenUseThemToPerformDatabaseValidationThenPass(workloadParams);
    }

    private void doShouldCreateValidationParametersThenUseThemToPerformDatabaseValidationThenPass(Map<String, String> workloadParams) throws ClientException, IOException, DriverConfigurationException {
        // **************************************************
        // where validation parameters should be written (ensure file does not yet exist)
        // **************************************************
        File validationParamsFile = temporaryFolder.newFile();
        assertThat(validationParamsFile.length(), is(0l));

        // **************************************************
        // configuration for generating validation parameters
        // **************************************************
        // DummyDb-specific parameters
        workloadParams.put(DummyLdbcSnbInteractiveDb.SLEEP_DURATION_NANO_ARG, Long.toString(TimeUnit.MILLISECONDS.toNanos(1)));
        workloadParams.put(DummyLdbcSnbInteractiveDb.SLEEP_TYPE_ARG, DummyLdbcSnbInteractiveDb.SleepType.SPIN.name());
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
                new ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions(validationParamsFile.getAbsolutePath(), 500);
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = false;
        long spinnerSleepDurationAsMilli = 0l;
        boolean printHelp = false;
        boolean ignoreScheduledStartTimes = false;
        boolean shouldCreateResultsLog = false;
        long warmupCount = 100;

        DriverConfiguration params = new ConsoleAndFileDriverConfiguration(
                workloadParams,
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
                shouldCreateResultsLog,
                warmupCount
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
                workloadParams,
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
                shouldCreateResultsLog,
                warmupCount
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

    private long percent(long value, double percent) {
        return Math.round(value * percent);
    }
}
