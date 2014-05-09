package com.ldbc.driver.generator;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcInteractiveWorkload;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.db.CsvDb;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class TimeMappingGeneratorTests {
    private final long RANDOM_SEED = 42;
    private GeneratorFactory generators = null;

    @Before
    public final void initGeneratorFactory() {
        generators = new GeneratorFactory(new RandomDataGeneratorFactory(RANDOM_SEED));
    }

    @Test
    public void shouldOffset() {
        // Given
        Function<Integer, Operation<?>> msToOperationFun = new Function<Integer, Operation<?>>() {
            @Override
            public Operation<?> apply(Integer timeMs) {
                Operation<?> operation = new Operation<Object>() {
                };
                operation.setScheduledStartTime(Time.fromMilli(timeMs));
                return operation;
            }
        };
        List<Operation<?>> operations = ImmutableList.copyOf(Iterators.transform(generators.boundedIncrementing(0, 100, 1000), msToOperationFun));
        assertThat(operations.size(), is(11));
        assertThat(operations.get(0).scheduledStartTime(), equalTo(Time.fromMilli(0)));
        assertThat(operations.get(10).scheduledStartTime(), equalTo(Time.fromMilli(1000)));
        Time newStartTime = Time.fromMilli(500);

        // When
        List<Operation<?>> offsetOperations = ImmutableList.copyOf(generators.timeOffset(operations.iterator(), newStartTime));

        // Then
        assertThat(offsetOperations.size(), is(11));
        assertThat(offsetOperations.get(0).scheduledStartTime(), equalTo(newStartTime));
        assertThat(offsetOperations.get(1).scheduledStartTime(), equalTo(Time.fromMilli(600)));
        assertThat(offsetOperations.get(2).scheduledStartTime(), equalTo(Time.fromMilli(700)));
        assertThat(offsetOperations.get(3).scheduledStartTime(), equalTo(Time.fromMilli(800)));
        assertThat(offsetOperations.get(4).scheduledStartTime(), equalTo(Time.fromMilli(900)));
        assertThat(offsetOperations.get(5).scheduledStartTime(), equalTo(Time.fromMilli(1000)));
        assertThat(offsetOperations.get(6).scheduledStartTime(), equalTo(Time.fromMilli(1100)));
        assertThat(offsetOperations.get(7).scheduledStartTime(), equalTo(Time.fromMilli(1200)));
        assertThat(offsetOperations.get(8).scheduledStartTime(), equalTo(Time.fromMilli(1300)));
        assertThat(offsetOperations.get(9).scheduledStartTime(), equalTo(Time.fromMilli(1400)));
        assertThat(offsetOperations.get(10).scheduledStartTime(), equalTo(Time.fromMilli(1500)));
    }

    @Test
    public void shouldOffsetAndCompress() {
        // Given
        Function<Integer, Operation<?>> msToOperationFun = new Function<Integer, Operation<?>>() {
            @Override
            public Operation<?> apply(Integer timeMs) {
                Operation<?> operation = new Operation<Object>() {
                };
                operation.setScheduledStartTime(Time.fromMilli(timeMs));
                return operation;
            }
        };
        List<Operation<?>> operations = ImmutableList.copyOf(Iterators.transform(generators.boundedIncrementing(0, 100, 1000), msToOperationFun));
        assertThat(operations.size(), is(11));
        assertThat(operations.get(0).scheduledStartTime(), equalTo(Time.fromMilli(0)));
        assertThat(operations.get(10).scheduledStartTime(), equalTo(Time.fromMilli(1000)));
        Time newStartTime = Time.fromMilli(500);
        Double compressionRatio = 0.2;

        // When
        List<Operation<?>> offsetAndCompressedOperations = ImmutableList.copyOf(generators.timeOffsetAndCompress(operations.iterator(), newStartTime, compressionRatio));

        // Then
        assertThat(offsetAndCompressedOperations.size(), is(11));
        assertThat(offsetAndCompressedOperations.get(0).scheduledStartTime(), equalTo(newStartTime));
        assertThat(offsetAndCompressedOperations.get(1).scheduledStartTime(), equalTo(Time.fromMilli(520)));
        assertThat(offsetAndCompressedOperations.get(2).scheduledStartTime(), equalTo(Time.fromMilli(540)));
        assertThat(offsetAndCompressedOperations.get(3).scheduledStartTime(), equalTo(Time.fromMilli(560)));
        assertThat(offsetAndCompressedOperations.get(4).scheduledStartTime(), equalTo(Time.fromMilli(580)));
        assertThat(offsetAndCompressedOperations.get(5).scheduledStartTime(), equalTo(Time.fromMilli(600)));
        assertThat(offsetAndCompressedOperations.get(6).scheduledStartTime(), equalTo(Time.fromMilli(620)));
        assertThat(offsetAndCompressedOperations.get(7).scheduledStartTime(), equalTo(Time.fromMilli(640)));
        assertThat(offsetAndCompressedOperations.get(8).scheduledStartTime(), equalTo(Time.fromMilli(660)));
        assertThat(offsetAndCompressedOperations.get(9).scheduledStartTime(), equalTo(Time.fromMilli(680)));
        assertThat(offsetAndCompressedOperations.get(10).scheduledStartTime(), equalTo(Time.fromMilli(700)));
    }

    @Test
    public void shouldNotBreakTheMonotonicallyIncreasingScheduledStartTimesOfOperationsFromLdbcWorkload() throws WorkloadException {
        Map<String, String> paramsMap = new HashMap<String, String>();
        // LDBC Interactive Workload-specific parameters
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_1_KEY, "1");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_2_KEY, "2");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_3_KEY, "3");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_4_KEY, "4");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_5_KEY, "5");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_6_KEY, "6");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_7_KEY, "7");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_8_KEY, "6");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_9_KEY, "5");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_10_KEY, "4");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_11_KEY, "3");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_12_KEY, "2");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_13_KEY, "1");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_14_KEY, "1");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_1_KEY, "false");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_2_KEY, "false");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_3_KEY, "false");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_4_KEY, "false");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_5_KEY, "false");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_6_KEY, "false");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_7_KEY, "false");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_8_KEY, "false");
        paramsMap.put(LdbcInteractiveWorkload.PARAMETERS_FILENAME_KEY, "ldbc_driver/workloads/ldbc/socnet/interactive/parameters.json");
        paramsMap.put(LdbcInteractiveWorkload.INTERLEAVE_DURATION_KEY, "10");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_STREAM_FILENAME_KEY, "ldbc_driver/workloads/ldbc/socnet/interactive/updates.csv");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_RATIO_KEY, "0");
        paramsMap.put(LdbcInteractiveWorkload.READ_RATIO_KEY, "1");
        // CsvDb-specific parameters
        String csvOutputFilePath = "temp_csv_output_file.csv";
        FileUtils.deleteQuietly(new File(csvOutputFilePath));
        paramsMap.put(CsvDb.CSV_PATH_KEY, csvOutputFilePath);
        // Driver-specific parameters
        String dbClassName = CsvDb.class.getName();
        String workloadClassName = LdbcInteractiveWorkload.class.getName();
        long operationCount = 10000;
        int threadCount = 1;
        boolean showStatus = true;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultFilePath = "test_write_to_csv_results.json";
        FileUtils.deleteQuietly(new File(resultFilePath));
        Double timeCompressionRatio = null;
        Duration gctDeltaDuration = Duration.fromSeconds(10);
        List<String> peerIds = Lists.newArrayList();
        Duration toleratedExecutionDelay = Duration.fromSeconds(1);

        assertThat(new File(csvOutputFilePath).exists(), is(false));
        assertThat(new File(resultFilePath).exists(), is(false));

        ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(paramsMap, dbClassName, workloadClassName, operationCount,
                threadCount, showStatus, timeUnit, resultFilePath, timeCompressionRatio, gctDeltaDuration, peerIds, toleratedExecutionDelay);

        Workload workload = new LdbcInteractiveWorkload();
        workload.init(configuration);
        List<Operation<?>> operations = Lists.newArrayList(workload.operations(new GeneratorFactory(new RandomDataGeneratorFactory(42L))));

        Time prevOperationScheduledStartTime = operations.get(0).scheduledStartTime().minus(Duration.fromMilli(1));
        for (Operation<?> operation : operations) {
            assertThat(operation.scheduledStartTime().gt(prevOperationScheduledStartTime), is(true));
            prevOperationScheduledStartTime = operation.scheduledStartTime();
        }

        Iterator<Operation<?>> offsetAndCompressedOperations = generators.timeOffsetAndCompress(operations.iterator(), Time.now().plus(Duration.fromMilli(500)), 1.0);
        Time prevOffsetOperationScheduledStartTime = operations.get(0).scheduledStartTime().minus(Duration.fromMilli(1));
        while (offsetAndCompressedOperations.hasNext()) {
            Operation<?> operation = offsetAndCompressedOperations.next();
            assertThat(operation.scheduledStartTime().gt(prevOffsetOperationScheduledStartTime), is(true));
            prevOffsetOperationScheduledStartTime = operation.scheduledStartTime();
        }
    }
}
