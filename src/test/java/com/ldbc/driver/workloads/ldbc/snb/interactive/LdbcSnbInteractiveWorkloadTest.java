package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.*;
import com.ldbc.driver.control.*;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.streams.IteratorSplitter;
import com.ldbc.driver.runtime.streams.IteratorSplittingException;
import com.ldbc.driver.runtime.streams.SplitDefinition;
import com.ldbc.driver.runtime.streams.SplitResult;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.*;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.CsvDb;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyDb;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveOperationInstances;
import org.apache.commons.io.FileUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class LdbcSnbInteractiveWorkloadTest {
    TimeSource TIME_SOURCE = new SystemTimeSource();

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
    public void shouldBeRepeatableWhenTwoIdenticalWorkloadsAreUsedWithIdenticalGeneratorFactories() throws ClientException, DriverConfigurationException, WorkloadException, IOException {
        // Given
        Map<String, String> paramsMap = new HashMap<>();
        // LDBC Interactive Workload-specific parameters
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_1_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_2_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_3_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_4_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_5_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_6_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_7_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_8_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_9_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_10_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_11_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_12_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_13_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_14_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_1_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_2_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_3_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_4_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_5_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_6_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_7_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_8_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_9_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_10_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_11_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_12_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_13_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_14_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_1_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_2_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_3_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_4_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_5_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_6_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_7_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_8_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        paramsMap.put(LdbcSnbInteractiveWorkload.DATA_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        // DummyDb-specific parameters
        paramsMap.put(DummyDb.SLEEP_DURATION_MILLI, Long.toString(Duration.fromMilli(100).asMilli()));
        // Driver-specific parameters
        String dbClassName = DummyDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 100;
        int threadCount = 1;
        boolean showStatus = true;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultFilePath = "test_ldbc_socnet_interactive_results.json";
        FileUtils.deleteQuietly(new File(resultFilePath));
        double timeCompressionRatio = 1.0;
        Duration gctDeltaDuration = Duration.fromSeconds(10);
        Set<String> peerIds = new HashSet<>();
        Duration toleratedExecutionDelay = Duration.fromMinutes(5);
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = true;
        Duration spinnerSleepDuration = Duration.fromMilli(0);

        assertThat(new File(resultFilePath).exists(), is(false));

        DriverConfiguration params = new ConsoleAndFileDriverConfiguration(paramsMap, dbClassName, workloadClassName, operationCount,
                threadCount, showStatus, timeUnit, resultFilePath, timeCompressionRatio, gctDeltaDuration, peerIds, toleratedExecutionDelay,
                validationParams, dbValidationFilePath, validateWorkload, calculateWorkloadStatistics, spinnerSleepDuration);

        String ldbcSnbDatagenUpdateStreamPropertiesPath = TestUtils.getResource("/updateStream_0.properties").getAbsolutePath();
        Properties ldbcSnbDatagenUpdateStreamProperties = new Properties();
        ldbcSnbDatagenUpdateStreamProperties.load(new FileInputStream(ldbcSnbDatagenUpdateStreamPropertiesPath));
        params = params.applyMap(MapUtils.<String, String>propertiesToMap(ldbcSnbDatagenUpdateStreamProperties));

        Workload workloadA = new LdbcSnbInteractiveWorkload();
        workloadA.init(params);

        Workload workloadB = new LdbcSnbInteractiveWorkload();
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

        workloadA.cleanup();
        workloadB.cleanup();
    }

    @Test
    public void shouldGenerateConfiguredQueryMix() throws ClientException, DriverConfigurationException, WorkloadException {
        // Given
        String ldbcSnbDatagenUpdateStreamPropertiesPath = TestUtils.getResource("/updateStream_0.properties").getAbsolutePath();
        String ldbcDriverPropertiesPath = TestUtils.getResource("/ldbc_driver_default.properties").getAbsolutePath();

        ConsoleAndFileDriverConfiguration params = ConsoleAndFileDriverConfiguration.fromArgs(new String[]{
                "-w", LdbcSnbInteractiveWorkload.class.getName(),
                "-P", ldbcSnbDatagenUpdateStreamPropertiesPath,
                "-P", ldbcDriverPropertiesPath,
                // database class is loaded by Client class, which is bypassed in this test
                "-db", "this will never be used",
                "-oc", "10000",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_1_INTERLEAVE_KEY, "100",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_2_INTERLEAVE_KEY, "200",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_3_INTERLEAVE_KEY, "400",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_4_INTERLEAVE_KEY, "800",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_5_INTERLEAVE_KEY, "1600",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_6_INTERLEAVE_KEY, "1600",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_7_INTERLEAVE_KEY, "800",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_8_INTERLEAVE_KEY, "800",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_9_INTERLEAVE_KEY, "400",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_10_INTERLEAVE_KEY, "200",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_11_INTERLEAVE_KEY, "200",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_12_INTERLEAVE_KEY, "200",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_13_INTERLEAVE_KEY, "100",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_14_INTERLEAVE_KEY, "100",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_1_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_2_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_3_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_4_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_5_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_6_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_7_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_8_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_9_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_10_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_11_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_12_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_13_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveWorkload.READ_OPERATION_14_ENABLE_KEY, "true",
                "-p", LdbcSnbInteractiveWorkload.WRITE_OPERATION_1_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveWorkload.WRITE_OPERATION_2_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveWorkload.WRITE_OPERATION_3_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveWorkload.WRITE_OPERATION_4_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveWorkload.WRITE_OPERATION_5_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveWorkload.WRITE_OPERATION_6_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveWorkload.WRITE_OPERATION_7_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveWorkload.WRITE_OPERATION_8_ENABLE_KEY, "false",
                "-p", LdbcSnbInteractiveWorkload.DATA_DIRECTORY, TestUtils.getResource("/").getAbsolutePath(),
                "-p", LdbcSnbInteractiveWorkload.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath()
        });

        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init(params);

        // When

        Iterator<Class> operationTypes = Iterators.transform(
                workload.operations(new GeneratorFactory(new RandomDataGeneratorFactory(42L))),
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
    public void shouldWriteToCsvWhileRunningWorkload() throws ClientException, DriverConfigurationException, WorkloadException, IOException {
        // Given
        Map<String, String> paramsMap = new HashMap<>();
        // LDBC Interactive Workload-specific parameters
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_1_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_2_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_3_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_4_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_5_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_6_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_7_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_8_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_9_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_10_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_11_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_12_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_13_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_14_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_1_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_2_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_3_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_4_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_5_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_6_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_7_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_8_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_9_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_10_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_11_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_12_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_13_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_14_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_1_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_2_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_3_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_4_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_5_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_6_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_7_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_8_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        paramsMap.put(LdbcSnbInteractiveWorkload.DATA_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        // CsvDb-specific parameters
        String csvOutputFilePath = "temp_csv_output_file.csv";
        FileUtils.deleteQuietly(new File(csvOutputFilePath));
        paramsMap.put(CsvDb.CSV_PATH_KEY, csvOutputFilePath);
        // Driver-specific parameters
        String dbClassName = CsvDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 1000;
        int threadCount = 1;
        boolean showStatus = true;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultFilePath = "test_write_to_csv_results.json";
        FileUtils.deleteQuietly(new File(resultFilePath));
        double timeCompressionRatio = 0.01;
        Duration gctDeltaDuration = Duration.fromSeconds(10);
        Set<String> peerIds = new HashSet<>();
        Duration toleratedExecutionDelay = Duration.fromSeconds(1);
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = true;
        Duration spinnerSleepDuration = Duration.fromMilli(0);

        assertThat(new File(csvOutputFilePath).exists(), is(false));
        assertThat(new File(resultFilePath).exists(), is(false));

        DriverConfiguration params = new ConsoleAndFileDriverConfiguration(paramsMap, dbClassName, workloadClassName, operationCount,
                threadCount, showStatus, timeUnit, resultFilePath, timeCompressionRatio, gctDeltaDuration, peerIds, toleratedExecutionDelay,
                validationParams, dbValidationFilePath, validateWorkload, calculateWorkloadStatistics, spinnerSleepDuration);

        String ldbcSnbDatagenUpdateStreamPropertiesPath = TestUtils.getResource("/updateStream_0.properties").getAbsolutePath();
        Properties ldbcSnbDatagenUpdateStreamProperties = new Properties();
        ldbcSnbDatagenUpdateStreamProperties.load(new FileInputStream(ldbcSnbDatagenUpdateStreamPropertiesPath));
        params = params.applyMap(MapUtils.<String, String>propertiesToMap(ldbcSnbDatagenUpdateStreamProperties));

        // When
        Client client = new Client(new LocalControlService(TIME_SOURCE.now().plus(Duration.fromSeconds(3)), params), TIME_SOURCE);
        client.start();

        // Then
        assertThat(new File(csvOutputFilePath).exists(), is(true));
        assertThat(new File(resultFilePath).exists(), is(true));
        FileUtils.deleteQuietly(new File(csvOutputFilePath));
        FileUtils.deleteQuietly(new File(resultFilePath));
        assertThat(new File(csvOutputFilePath).exists(), is(false));
        assertThat(new File(resultFilePath).exists(), is(false));
    }

    @Test
    public void shouldLoadFromConfigFile() throws DriverConfigurationException, ClientException {
        String ldbcSnbDatagenUpdateStreamPropertiesPath = TestUtils.getResource("/updateStream_0.properties").getAbsolutePath();
        String ldbcSocnetInteractiveTestPropertiesPath =
                new File(ConfigurationFileTestHelper.getWorkloadsDirectory(), "ldbc/socnet/interactive/ldbc_socnet_interactive.properties").getAbsolutePath();
        String ldbcDriverTestPropertiesPath =
                TestUtils.getResource("/ldbc_driver_default.properties").getAbsolutePath();

        String csvOutputFilePath = "temp_csv_output_file.csv";
        FileUtils.deleteQuietly(new File(csvOutputFilePath));
        String resultFilePath = "test_write_to_csv_results.json";

        assertThat(new File(csvOutputFilePath).exists(), is(false));
        assertThat(new File(resultFilePath).exists(), is(false));

        assertThat(new File(ldbcSocnetInteractiveTestPropertiesPath).exists(), is(true));
        assertThat(new File(ldbcDriverTestPropertiesPath).exists(), is(true));

        ConsoleAndFileDriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromArgs(new String[]{
                "-" + ConsoleAndFileDriverConfiguration.CALCULATE_WORKLOAD_STATISTICS_ARG,
                "-" + ConsoleAndFileDriverConfiguration.RESULT_FILE_PATH_ARG, resultFilePath,
                "-" + ConsoleAndFileDriverConfiguration.DB_ARG, CsvDb.class.getName(),
                "-p", LdbcSnbInteractiveWorkload.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath(),
                "-p", LdbcSnbInteractiveWorkload.DATA_DIRECTORY, TestUtils.getResource("/").getAbsolutePath(),
                "-p", CsvDb.CSV_PATH_KEY, csvOutputFilePath,
                "-P", ldbcSnbDatagenUpdateStreamPropertiesPath,
                "-P", ldbcSocnetInteractiveTestPropertiesPath,
                "-P", ldbcDriverTestPropertiesPath});


        assertThat(new File(csvOutputFilePath).exists(), is(false));
        assertThat(new File(resultFilePath).exists(), is(false));


        // When
        Client client = new Client(new LocalControlService(TIME_SOURCE.now().plus(Duration.fromMilli(500)), configuration), TIME_SOURCE);
        client.start();

        // Then
        assertThat(new File(csvOutputFilePath).exists(), is(true));
        assertThat(new File(resultFilePath).exists(), is(true));
        FileUtils.deleteQuietly(new File(csvOutputFilePath));
        FileUtils.deleteQuietly(new File(resultFilePath));
        assertThat(new File(csvOutputFilePath).exists(), is(false));
        assertThat(new File(resultFilePath).exists(), is(false));
    }

    @Test
    public void shouldAssignMonotonicallyIncreasingScheduledStartTimesToOperations() throws WorkloadException, IOException, DriverConfigurationException {
        Map<String, String> paramsMap = new HashMap<>();
        // LDBC Interactive Workload-specific parameters
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_1_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_2_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_3_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_4_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_5_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_6_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_7_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_8_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_9_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_10_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_11_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_12_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_13_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_14_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_1_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_2_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_3_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_4_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_5_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_6_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_7_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_8_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_9_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_10_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_11_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_12_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_13_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_14_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_1_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_2_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_3_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_4_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_5_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_6_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_7_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_8_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        paramsMap.put(LdbcSnbInteractiveWorkload.DATA_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        // CsvDb-specific parameters
        String csvOutputFilePath = "temp_csv_output_file.csv";
        FileUtils.deleteQuietly(new File(csvOutputFilePath));
        paramsMap.put(CsvDb.CSV_PATH_KEY, csvOutputFilePath);
        // Driver-specific parameters
        String dbClassName = CsvDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 1000000;
        int threadCount = 1;
        boolean showStatus = true;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultFilePath = "test_write_to_csv_results.json";
        FileUtils.deleteQuietly(new File(resultFilePath));
        double timeCompressionRatio = 0.01;
        Duration gctDeltaDuration = Duration.fromSeconds(10);
        Set<String> peerIds = new HashSet<>();
        Duration toleratedExecutionDelay = Duration.fromSeconds(1);
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = true;
        Duration spinnerSleepDuration = Duration.fromMilli(0);

        assertThat(new File(csvOutputFilePath).exists(), is(false));
        assertThat(new File(resultFilePath).exists(), is(false));

        DriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(paramsMap, dbClassName, workloadClassName, operationCount,
                threadCount, showStatus, timeUnit, resultFilePath, timeCompressionRatio, gctDeltaDuration, peerIds, toleratedExecutionDelay,
                validationParams, dbValidationFilePath, validateWorkload, calculateWorkloadStatistics, spinnerSleepDuration);

        String ldbcSnbDatagenUpdateStreamPropertiesPath = TestUtils.getResource("/updateStream_0.properties").getAbsolutePath();
        Properties ldbcSnbDatagenUpdateStreamProperties = new Properties();
        ldbcSnbDatagenUpdateStreamProperties.load(new FileInputStream(ldbcSnbDatagenUpdateStreamPropertiesPath));
        configuration = configuration.applyMap(MapUtils.<String, String>propertiesToMap(ldbcSnbDatagenUpdateStreamProperties));

        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init(configuration);
        List<Operation<?>> operations = Lists.newArrayList(workload.operations(new GeneratorFactory(new RandomDataGeneratorFactory(42L))));

        Time prevOperationScheduledStartTime = operations.get(0).scheduledStartTime().minus(Duration.fromMilli(1));
        for (Operation<?> operation : operations) {
            assertThat(operation.scheduledStartTime().gte(prevOperationScheduledStartTime), is(true));
            prevOperationScheduledStartTime = operation.scheduledStartTime();
        }

        workload.cleanup();
    }

    @Ignore
    @Test
    public void shouldWorkWhenOnlyWriteOperationsAreEnabled() throws WorkloadException, ClientException, IOException, DriverConfigurationException {
        Map<String, String> params = new HashMap<>();
        // LDBC Interactive Workload-specific parameters
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_1_INTERLEAVE_KEY, "10");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_2_INTERLEAVE_KEY, "10");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_3_INTERLEAVE_KEY, "10");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_4_INTERLEAVE_KEY, "10");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_5_INTERLEAVE_KEY, "10");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_6_INTERLEAVE_KEY, "10");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_7_INTERLEAVE_KEY, "10");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_8_INTERLEAVE_KEY, "10");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_9_INTERLEAVE_KEY, "10");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_10_INTERLEAVE_KEY, "10");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_11_INTERLEAVE_KEY, "10");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_12_INTERLEAVE_KEY, "10");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_13_INTERLEAVE_KEY, "10");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_14_INTERLEAVE_KEY, "10");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_1_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_2_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_3_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_4_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_5_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_6_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_7_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_8_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_9_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_10_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_11_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_12_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_13_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_14_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_1_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_2_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_3_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_4_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_5_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_6_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_7_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_8_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        params.put(LdbcSnbInteractiveWorkload.DATA_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        // CsvDb-specific parameters
        params.put(DummyDb.SLEEP_DURATION_MILLI, Long.toString(Duration.fromMilli(1).asMilli()));
        // Driver-specific parameters
        String dbClassName = DummyDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 500;
        int threadCount = 1;
        boolean showStatus = true;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultFilePath = "test_write_to_csv_results.json";
        FileUtils.deleteQuietly(new File(resultFilePath));
        double timeCompressionRatio = 0.01;
        Duration gctDeltaDuration = Duration.fromSeconds(10);
        Set<String> peerIds = new HashSet<>();
        Duration toleratedExecutionDelay = Duration.fromSeconds(100);
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = true;
        Duration spinnerSleepDuration = Duration.fromMilli(0);

        assertThat(new File(resultFilePath).exists(), is(false));

        DriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(params, dbClassName, workloadClassName, operationCount,
                threadCount, showStatus, timeUnit, resultFilePath, timeCompressionRatio, gctDeltaDuration, peerIds, toleratedExecutionDelay,
                validationParams, dbValidationFilePath, validateWorkload, calculateWorkloadStatistics, spinnerSleepDuration);

//        String ldbcSnbDatagenUpdateStreamPropertiesPath = TestUtils.getResource("/updateStream_0.properties").getAbsolutePath();
//        Properties ldbcSnbDatagenUpdateStreamProperties = new Properties();
//        ldbcSnbDatagenUpdateStreamProperties.load(new FileInputStream(ldbcSnbDatagenUpdateStreamPropertiesPath));
//        configuration = configuration.applyMap(MapUtils.<String, String>propertiesToMap(ldbcSnbDatagenUpdateStreamProperties));

        Client client = new Client(new LocalControlService(TIME_SOURCE.now().plus(Duration.fromSeconds(3)), configuration), TIME_SOURCE);
        client.start();

        assertThat(new File(resultFilePath).exists(), is(true));
        FileUtils.deleteQuietly(new File(resultFilePath));
    }

    @Test
    public void operationsShouldHaveMonotonicallyIncreasingScheduledStartTimesAfterSplitting() throws WorkloadException, IOException, DriverConfigurationException {
        Map<String, String> paramsMap = new HashMap<>();
        // LDBC Interactive Workload-specific parameters
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_1_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_2_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_3_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_4_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_5_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_6_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_7_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_8_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_9_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_10_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_11_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_12_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_13_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_14_INTERLEAVE_KEY, "10");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_1_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_2_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_3_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_4_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_5_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_6_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_7_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_8_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_9_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_10_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_11_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_12_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_13_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_14_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_1_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_2_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_3_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_4_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_5_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_6_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_7_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_8_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        paramsMap.put(LdbcSnbInteractiveWorkload.DATA_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        // CsvDb-specific parameters
        String csvOutputFilePath = "temp_csv_output_file.csv";
        FileUtils.deleteQuietly(new File(csvOutputFilePath));
        paramsMap.put(CsvDb.CSV_PATH_KEY, csvOutputFilePath);
        // Driver-specific parameters
        String dbClassName = CsvDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 1000000;
        int threadCount = 1;
        boolean showStatus = true;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultFilePath = "test_write_to_csv_results.json";
        FileUtils.deleteQuietly(new File(resultFilePath));
        double timeCompressionRatio = 1.0;
        Duration gctDeltaDuration = Duration.fromSeconds(10);
        Set<String> peerIds = new HashSet<>();
        Duration toleratedExecutionDelay = Duration.fromSeconds(1);
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = true;
        Duration spinnerSleepDuration = Duration.fromMilli(0);

        assertThat(new File(csvOutputFilePath).exists(), is(false));
        assertThat(new File(resultFilePath).exists(), is(false));

        DriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(paramsMap, dbClassName, workloadClassName, operationCount,
                threadCount, showStatus, timeUnit, resultFilePath, timeCompressionRatio, gctDeltaDuration, peerIds, toleratedExecutionDelay,
                validationParams, dbValidationFilePath, validateWorkload, calculateWorkloadStatistics, spinnerSleepDuration);

        String ldbcSnbDatagenUpdateStreamPropertiesPath = TestUtils.getResource("/updateStream_0.properties").getAbsolutePath();
        Properties ldbcSnbDatagenUpdateStreamProperties = new Properties();
        ldbcSnbDatagenUpdateStreamProperties.load(new FileInputStream(ldbcSnbDatagenUpdateStreamPropertiesPath));
        configuration = configuration.applyMap(MapUtils.<String, String>propertiesToMap(ldbcSnbDatagenUpdateStreamProperties));

        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init(configuration);
        List<Operation<?>> operations = Lists.newArrayList(workload.operations(new GeneratorFactory(new RandomDataGeneratorFactory(42L))));

        Time firstOperationScheduledStartTime = operations.get(0).scheduledStartTime();

        Time prevOperationScheduledStartTime = firstOperationScheduledStartTime.minus(Duration.fromMilli(1));
        for (Operation<?> operation : Lists.newArrayList(operations)) {
            assertThat(operation.scheduledStartTime().gte(prevOperationScheduledStartTime), is(true));
            prevOperationScheduledStartTime = operation.scheduledStartTime();
        }

        List<Operation<?>> windowedOperations;
        List<Operation<?>> blockingOperations;
        List<Operation<?>> asynchronousOperations;
        try {
            IteratorSplitter<Operation<?>> splitter = new IteratorSplitter<>(IteratorSplitter.UnmappedItemPolicy.ABORT);
            SplitDefinition<Operation<?>> windowed = new SplitDefinition<>(Workload.operationTypesBySchedulingMode(workload.operationClassifications(), OperationClassification.SchedulingMode.WINDOWED));
            SplitDefinition<Operation<?>> blocking = new SplitDefinition<>(Workload.operationTypesBySchedulingMode(workload.operationClassifications(), OperationClassification.SchedulingMode.INDIVIDUAL_BLOCKING));
            SplitDefinition<Operation<?>> asynchronous = new SplitDefinition<>(Workload.operationTypesBySchedulingMode(workload.operationClassifications(), OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC));
            SplitResult splits = splitter.split(operations.iterator(), windowed, blocking, asynchronous);
            windowedOperations = Lists.newArrayList(splits.getSplitFor(windowed));
            blockingOperations = Lists.newArrayList(splits.getSplitFor(blocking));
            asynchronousOperations = Lists.newArrayList(splits.getSplitFor(asynchronous));
        } catch (IteratorSplittingException e) {
            throw new WorkloadException(
                    String.format("Error while splitting operation stream by scheduling mode\n%s", ConcurrentErrorReporter.stackTraceToString(e)),
                    e);
        }

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

    @Ignore
    @Test
    public void shouldPassWorkloadValidation() {
        assertThat(true, is(false));
    }

    @Ignore
    @Test
    public void shouldNotFailUnexpectedlyWhenQueriesAreLongRunning() throws ClientException, DriverConfigurationException, WorkloadException, IOException {
        // Given
        Map<String, String> paramsMap = new HashMap<>();
        // LDBC Interactive Workload-specific parameters
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_1_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_2_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_3_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_4_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_5_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_6_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_7_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_8_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_9_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_10_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_11_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_12_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_13_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_14_INTERLEAVE_KEY, "100");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_1_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_2_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_3_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_4_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_5_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_6_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_7_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_8_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_9_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_10_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_11_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_12_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_13_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.READ_OPERATION_14_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_1_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_2_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_3_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_4_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_5_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_6_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_7_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_8_ENABLE_KEY, "true");
        paramsMap.put(LdbcSnbInteractiveWorkload.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        paramsMap.put(LdbcSnbInteractiveWorkload.DATA_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        // DummyDb-specific parameters
        paramsMap.put(DummyDb.SLEEP_DURATION_MILLI, Long.toString(Duration.fromSeconds(40).asMilli()));
        // Driver-specific parameters
        String dbClassName = DummyDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 5;
        int threadCount = 2;
        boolean showStatus = true;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultFilePath = "test_write_to_csv_results.json";
        FileUtils.deleteQuietly(new File(resultFilePath));
        double timeCompressionRatio = 1.0;
        Duration gctDeltaDuration = Duration.fromSeconds(10);
        Set<String> peerIds = new HashSet<>();
        Duration toleratedExecutionDelay = Duration.fromMinutes(5);
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = true;
        Duration spinnerSleepDuration = Duration.fromMilli(0);

        assertThat(new File(resultFilePath).exists(), is(false));

        DriverConfiguration params = new ConsoleAndFileDriverConfiguration(paramsMap, dbClassName, workloadClassName, operationCount,
                threadCount, showStatus, timeUnit, resultFilePath, timeCompressionRatio, gctDeltaDuration, peerIds, toleratedExecutionDelay,
                validationParams, dbValidationFilePath, validateWorkload, calculateWorkloadStatistics, spinnerSleepDuration);

        String ldbcSnbDatagenUpdateStreamPropertiesPath = TestUtils.getResource("/updateStream_0.properties").getAbsolutePath();
        Properties ldbcSnbDatagenUpdateStreamProperties = new Properties();
        ldbcSnbDatagenUpdateStreamProperties.load(new FileInputStream(ldbcSnbDatagenUpdateStreamPropertiesPath));
        params = params.applyMap(MapUtils.<String, String>propertiesToMap(ldbcSnbDatagenUpdateStreamProperties));

        // When
        Client client = new Client(new LocalControlService(TIME_SOURCE.now().plus(Duration.fromMilli(500)), params), TIME_SOURCE);
        client.start();

        // Then
        assertThat(new File(resultFilePath).exists(), is(true));
        FileUtils.deleteQuietly(new File(resultFilePath));
        assertThat(new File(resultFilePath).exists(), is(false));
    }
}
