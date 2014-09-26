package com.ldbc.driver.generator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.testutils.TestUtils;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1Factory;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.CsvWritingLdbcSnbInteractiveDb;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class TimeMappingOperationGeneratorTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private TimeSource TIME_SOURCE = new SystemTimeSource();
    private final long RANDOM_SEED = 42;
    private GeneratorFactory gf = null;

    @Before
    public final void initGeneratorFactory() {
        gf = new GeneratorFactory(new RandomDataGeneratorFactory(RANDOM_SEED));
    }

    @Test
    public void shouldOffset() {
        // Given
        Iterator<Operation<?>> operations = gf.limit(
                new TimedNamedOperation1Factory(
                        // start times
                        gf.constantIncrementTime(Time.fromMilli(0), Duration.fromMilli(100)),
                        // dependency times
                        gf.constantIncrementTime(Time.fromMilli(0), Duration.fromMilli(50)),
                        // names
                        gf.constant("name1")
                ),
                11
        );
        List<Operation<?>> operationsList = ImmutableList.copyOf(operations);
        assertThat(operationsList.size(), is(11));
        assertThat(operationsList.get(0).scheduledStartTime(), equalTo(Time.fromMilli(0)));
        assertThat(operationsList.get(0).dependencyTime(), equalTo(Time.fromMilli(0)));
        assertThat(operationsList.get(1).scheduledStartTime(), equalTo(Time.fromMilli(100)));
        assertThat(operationsList.get(1).dependencyTime(), equalTo(Time.fromMilli(50)));
        assertThat(operationsList.get(2).scheduledStartTime(), equalTo(Time.fromMilli(200)));
        assertThat(operationsList.get(2).dependencyTime(), equalTo(Time.fromMilli(100)));
        assertThat(operationsList.get(3).scheduledStartTime(), equalTo(Time.fromMilli(300)));
        assertThat(operationsList.get(3).dependencyTime(), equalTo(Time.fromMilli(150)));
        assertThat(operationsList.get(4).scheduledStartTime(), equalTo(Time.fromMilli(400)));
        assertThat(operationsList.get(4).dependencyTime(), equalTo(Time.fromMilli(200)));
        assertThat(operationsList.get(5).scheduledStartTime(), equalTo(Time.fromMilli(500)));
        assertThat(operationsList.get(5).dependencyTime(), equalTo(Time.fromMilli(250)));
        assertThat(operationsList.get(6).scheduledStartTime(), equalTo(Time.fromMilli(600)));
        assertThat(operationsList.get(6).dependencyTime(), equalTo(Time.fromMilli(300)));
        assertThat(operationsList.get(7).scheduledStartTime(), equalTo(Time.fromMilli(700)));
        assertThat(operationsList.get(7).dependencyTime(), equalTo(Time.fromMilli(350)));
        assertThat(operationsList.get(8).scheduledStartTime(), equalTo(Time.fromMilli(800)));
        assertThat(operationsList.get(8).dependencyTime(), equalTo(Time.fromMilli(400)));
        assertThat(operationsList.get(9).scheduledStartTime(), equalTo(Time.fromMilli(900)));
        assertThat(operationsList.get(9).dependencyTime(), equalTo(Time.fromMilli(450)));
        assertThat(operationsList.get(10).scheduledStartTime(), equalTo(Time.fromMilli(1000)));
        assertThat(operationsList.get(10).dependencyTime(), equalTo(Time.fromMilli(500)));

        // When
        Time newStartTime = Time.fromMilli(500);
        List<Operation<?>> offsetOperationsList = ImmutableList.copyOf(gf.timeOffset(operationsList.iterator(), newStartTime));

        // Then
        assertThat(offsetOperationsList.size(), is(11));
        assertThat(offsetOperationsList.get(0).scheduledStartTime(), equalTo(Time.fromMilli(500)));
        assertThat(offsetOperationsList.get(0).dependencyTime(), equalTo(Time.fromMilli(500)));
        assertThat(offsetOperationsList.get(1).scheduledStartTime(), equalTo(Time.fromMilli(600)));
        assertThat(offsetOperationsList.get(1).dependencyTime(), equalTo(Time.fromMilli(550)));
        assertThat(offsetOperationsList.get(2).scheduledStartTime(), equalTo(Time.fromMilli(700)));
        assertThat(offsetOperationsList.get(2).dependencyTime(), equalTo(Time.fromMilli(600)));
        assertThat(offsetOperationsList.get(3).scheduledStartTime(), equalTo(Time.fromMilli(800)));
        assertThat(offsetOperationsList.get(3).dependencyTime(), equalTo(Time.fromMilli(650)));
        assertThat(offsetOperationsList.get(4).scheduledStartTime(), equalTo(Time.fromMilli(900)));
        assertThat(offsetOperationsList.get(4).dependencyTime(), equalTo(Time.fromMilli(700)));
        assertThat(offsetOperationsList.get(5).scheduledStartTime(), equalTo(Time.fromMilli(1000)));
        assertThat(offsetOperationsList.get(5).dependencyTime(), equalTo(Time.fromMilli(750)));
        assertThat(offsetOperationsList.get(6).scheduledStartTime(), equalTo(Time.fromMilli(1100)));
        assertThat(offsetOperationsList.get(6).dependencyTime(), equalTo(Time.fromMilli(800)));
        assertThat(offsetOperationsList.get(7).scheduledStartTime(), equalTo(Time.fromMilli(1200)));
        assertThat(offsetOperationsList.get(7).dependencyTime(), equalTo(Time.fromMilli(850)));
        assertThat(offsetOperationsList.get(8).scheduledStartTime(), equalTo(Time.fromMilli(1300)));
        assertThat(offsetOperationsList.get(8).dependencyTime(), equalTo(Time.fromMilli(900)));
        assertThat(offsetOperationsList.get(9).scheduledStartTime(), equalTo(Time.fromMilli(1400)));
        assertThat(offsetOperationsList.get(9).dependencyTime(), equalTo(Time.fromMilli(950)));
        assertThat(offsetOperationsList.get(10).scheduledStartTime(), equalTo(Time.fromMilli(1500)));
        assertThat(offsetOperationsList.get(10).dependencyTime(), equalTo(Time.fromMilli(1000)));
    }

    @Test
    public void shouldOffsetAndCompress() {
        // Given
        Iterator<Operation<?>> operations = gf.limit(
                new TimedNamedOperation1Factory(
                        // start times
                        gf.constantIncrementTime(Time.fromMilli(1000), Duration.fromMilli(100)),
                        // dependency times
                        gf.constantIncrementTime(Time.fromMilli(900), Duration.fromMilli(50)),
                        // names
                        gf.constant("name1")
                ),
                11
        );
        List<Operation<?>> operationsList = ImmutableList.copyOf(operations);

        assertThat(operationsList.size(), is(11));
        assertThat(operationsList.get(0).scheduledStartTime(), equalTo(Time.fromMilli(1000)));
        assertThat(operationsList.get(0).dependencyTime(), equalTo(Time.fromMilli(900)));
        assertThat(operationsList.get(1).scheduledStartTime(), equalTo(Time.fromMilli(1100)));
        assertThat(operationsList.get(1).dependencyTime(), equalTo(Time.fromMilli(950)));
        assertThat(operationsList.get(2).scheduledStartTime(), equalTo(Time.fromMilli(1200)));
        assertThat(operationsList.get(2).dependencyTime(), equalTo(Time.fromMilli(1000)));
        assertThat(operationsList.get(3).scheduledStartTime(), equalTo(Time.fromMilli(1300)));
        assertThat(operationsList.get(3).dependencyTime(), equalTo(Time.fromMilli(1050)));
        assertThat(operationsList.get(4).scheduledStartTime(), equalTo(Time.fromMilli(1400)));
        assertThat(operationsList.get(4).dependencyTime(), equalTo(Time.fromMilli(1100)));
        assertThat(operationsList.get(5).scheduledStartTime(), equalTo(Time.fromMilli(1500)));
        assertThat(operationsList.get(5).dependencyTime(), equalTo(Time.fromMilli(1150)));
        assertThat(operationsList.get(6).scheduledStartTime(), equalTo(Time.fromMilli(1600)));
        assertThat(operationsList.get(6).dependencyTime(), equalTo(Time.fromMilli(1200)));
        assertThat(operationsList.get(7).scheduledStartTime(), equalTo(Time.fromMilli(1700)));
        assertThat(operationsList.get(7).dependencyTime(), equalTo(Time.fromMilli(1250)));
        assertThat(operationsList.get(8).scheduledStartTime(), equalTo(Time.fromMilli(1800)));
        assertThat(operationsList.get(8).dependencyTime(), equalTo(Time.fromMilli(1300)));
        assertThat(operationsList.get(9).scheduledStartTime(), equalTo(Time.fromMilli(1900)));
        assertThat(operationsList.get(9).dependencyTime(), equalTo(Time.fromMilli(1350)));
        assertThat(operationsList.get(10).scheduledStartTime(), equalTo(Time.fromMilli(2000)));
        assertThat(operationsList.get(10).dependencyTime(), equalTo(Time.fromMilli(1400)));

        // When
        Time newStartTime = Time.fromMilli(500);
        Double compressionRatio = 0.2;
        List<Operation<?>> offsetAndCompressedOperationsList = ImmutableList.copyOf(gf.timeOffsetAndCompress(operationsList.iterator(), newStartTime, compressionRatio));

        // Then
        assertThat(offsetAndCompressedOperationsList.size(), is(11));
        assertThat(offsetAndCompressedOperationsList.get(0).scheduledStartTime(), equalTo(Time.fromMilli(500)));
        assertThat(offsetAndCompressedOperationsList.get(0).dependencyTime(), equalTo(Time.fromMilli(400)));
        assertThat(offsetAndCompressedOperationsList.get(1).scheduledStartTime(), equalTo(Time.fromMilli(520)));
        assertThat(offsetAndCompressedOperationsList.get(1).dependencyTime(), equalTo(Time.fromMilli(410)));
        assertThat(offsetAndCompressedOperationsList.get(2).scheduledStartTime(), equalTo(Time.fromMilli(540)));
        assertThat(offsetAndCompressedOperationsList.get(2).dependencyTime(), equalTo(Time.fromMilli(420)));
        assertThat(offsetAndCompressedOperationsList.get(3).scheduledStartTime(), equalTo(Time.fromMilli(560)));
        assertThat(offsetAndCompressedOperationsList.get(3).dependencyTime(), equalTo(Time.fromMilli(430)));
        assertThat(offsetAndCompressedOperationsList.get(4).scheduledStartTime(), equalTo(Time.fromMilli(580)));
        assertThat(offsetAndCompressedOperationsList.get(4).dependencyTime(), equalTo(Time.fromMilli(440)));
        assertThat(offsetAndCompressedOperationsList.get(5).scheduledStartTime(), equalTo(Time.fromMilli(600)));
        assertThat(offsetAndCompressedOperationsList.get(5).dependencyTime(), equalTo(Time.fromMilli(450)));
        assertThat(offsetAndCompressedOperationsList.get(6).scheduledStartTime(), equalTo(Time.fromMilli(620)));
        assertThat(offsetAndCompressedOperationsList.get(6).dependencyTime(), equalTo(Time.fromMilli(460)));
        assertThat(offsetAndCompressedOperationsList.get(7).scheduledStartTime(), equalTo(Time.fromMilli(640)));
        assertThat(offsetAndCompressedOperationsList.get(7).dependencyTime(), equalTo(Time.fromMilli(470)));
        assertThat(offsetAndCompressedOperationsList.get(8).scheduledStartTime(), equalTo(Time.fromMilli(660)));
        assertThat(offsetAndCompressedOperationsList.get(8).dependencyTime(), equalTo(Time.fromMilli(480)));
        assertThat(offsetAndCompressedOperationsList.get(9).scheduledStartTime(), equalTo(Time.fromMilli(680)));
        assertThat(offsetAndCompressedOperationsList.get(9).dependencyTime(), equalTo(Time.fromMilli(490)));
        assertThat(offsetAndCompressedOperationsList.get(10).scheduledStartTime(), equalTo(Time.fromMilli(700)));
        assertThat(offsetAndCompressedOperationsList.get(10).dependencyTime(), equalTo(Time.fromMilli(500)));
    }

    @Test
    public void shouldOffsetAndCompressWhenTimesAreVeryCloseTogetherWithoutRoundingErrors() {
        // Given
        Iterator<Operation<?>> operations = gf.limit(
                new TimedNamedOperation1Factory(
                        // start times
                        gf.constantIncrementTime(Time.fromNano(0), Duration.fromNano(1)),
                        // dependency times
                        gf.constantIncrementTime(Time.fromNano(0), Duration.fromNano(0)),
                        // names
                        gf.constant("name1")
                ),
                11
        );
        List<Operation<?>> operationsList = ImmutableList.copyOf(operations);

        assertThat(operationsList.size(), is(11));
        assertThat(operationsList.get(0).scheduledStartTime(), equalTo(Time.fromNano(0)));
        assertThat(operationsList.get(0).dependencyTime(), equalTo(Time.fromNano(0)));
        assertThat(operationsList.get(1).scheduledStartTime(), equalTo(Time.fromNano(1)));
        assertThat(operationsList.get(1).dependencyTime(), equalTo(Time.fromNano(0)));
        assertThat(operationsList.get(2).scheduledStartTime(), equalTo(Time.fromNano(2)));
        assertThat(operationsList.get(2).dependencyTime(), equalTo(Time.fromNano(0)));
        assertThat(operationsList.get(3).scheduledStartTime(), equalTo(Time.fromNano(3)));
        assertThat(operationsList.get(3).dependencyTime(), equalTo(Time.fromNano(0)));
        assertThat(operationsList.get(4).scheduledStartTime(), equalTo(Time.fromNano(4)));
        assertThat(operationsList.get(4).dependencyTime(), equalTo(Time.fromNano(0)));
        assertThat(operationsList.get(5).scheduledStartTime(), equalTo(Time.fromNano(5)));
        assertThat(operationsList.get(5).dependencyTime(), equalTo(Time.fromNano(0)));
        assertThat(operationsList.get(6).scheduledStartTime(), equalTo(Time.fromNano(6)));
        assertThat(operationsList.get(6).dependencyTime(), equalTo(Time.fromNano(0)));
        assertThat(operationsList.get(7).scheduledStartTime(), equalTo(Time.fromNano(7)));
        assertThat(operationsList.get(7).dependencyTime(), equalTo(Time.fromNano(0)));
        assertThat(operationsList.get(8).scheduledStartTime(), equalTo(Time.fromNano(8)));
        assertThat(operationsList.get(8).dependencyTime(), equalTo(Time.fromNano(0)));
        assertThat(operationsList.get(9).scheduledStartTime(), equalTo(Time.fromNano(9)));
        assertThat(operationsList.get(9).dependencyTime(), equalTo(Time.fromNano(0)));
        assertThat(operationsList.get(10).scheduledStartTime(), equalTo(Time.fromNano(10)));
        assertThat(operationsList.get(10).dependencyTime(), equalTo(Time.fromNano(0)));

        // When
        Time newStartTime = Time.fromNano(0);
        Double compressionRatio = 0.5;
        List<Operation<?>> offsetAndCompressedOperations = ImmutableList.copyOf(gf.timeOffsetAndCompress(operationsList.iterator(), newStartTime, compressionRatio));

        // Then
        assertThat(offsetAndCompressedOperations.size(), is(11));
        assertThat(offsetAndCompressedOperations.get(0).scheduledStartTime(), equalTo(Time.fromNano(0)));
        assertThat(offsetAndCompressedOperations.get(0).dependencyTime(), equalTo(Time.fromNano(0)));
        assertThat(offsetAndCompressedOperations.get(1).scheduledStartTime(), equalTo(Time.fromNano(1)));
        assertThat(offsetAndCompressedOperations.get(1).dependencyTime(), equalTo(Time.fromNano(0)));
        assertThat(offsetAndCompressedOperations.get(2).scheduledStartTime(), equalTo(Time.fromNano(1)));
        assertThat(offsetAndCompressedOperations.get(2).dependencyTime(), equalTo(Time.fromNano(0)));
        assertThat(offsetAndCompressedOperations.get(3).scheduledStartTime(), equalTo(Time.fromNano(2)));
        assertThat(offsetAndCompressedOperations.get(3).dependencyTime(), equalTo(Time.fromNano(0)));
        assertThat(offsetAndCompressedOperations.get(4).scheduledStartTime(), equalTo(Time.fromNano(2)));
        assertThat(offsetAndCompressedOperations.get(4).dependencyTime(), equalTo(Time.fromNano(0)));
        assertThat(offsetAndCompressedOperations.get(5).scheduledStartTime(), equalTo(Time.fromNano(3)));
        assertThat(offsetAndCompressedOperations.get(5).dependencyTime(), equalTo(Time.fromNano(0)));
        assertThat(offsetAndCompressedOperations.get(6).scheduledStartTime(), equalTo(Time.fromNano(3)));
        assertThat(offsetAndCompressedOperations.get(6).dependencyTime(), equalTo(Time.fromNano(0)));
        assertThat(offsetAndCompressedOperations.get(7).scheduledStartTime(), equalTo(Time.fromNano(4)));
        assertThat(offsetAndCompressedOperations.get(7).dependencyTime(), equalTo(Time.fromNano(0)));
        assertThat(offsetAndCompressedOperations.get(8).scheduledStartTime(), equalTo(Time.fromNano(4)));
        assertThat(offsetAndCompressedOperations.get(8).dependencyTime(), equalTo(Time.fromNano(0)));
        assertThat(offsetAndCompressedOperations.get(9).scheduledStartTime(), equalTo(Time.fromNano(5)));
        assertThat(offsetAndCompressedOperations.get(9).dependencyTime(), equalTo(Time.fromNano(0)));
        assertThat(offsetAndCompressedOperations.get(10).scheduledStartTime(), equalTo(Time.fromNano(5)));
        assertThat(offsetAndCompressedOperations.get(10).dependencyTime(), equalTo(Time.fromNano(0)));
    }

    @Test
    public void shouldNotBreakTheMonotonicallyIncreasingScheduledStartTimesOfOperationsFromLdbcWorkload() throws WorkloadException, IOException {
        Map<String, String> paramsMap = LdbcSnbInteractiveWorkload.defaultConfig();
        // LDBC Interactive Workload-specific parameters
        paramsMap.put(LdbcSnbInteractiveWorkload.DATA_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        paramsMap.put(LdbcSnbInteractiveWorkload.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        // CsvDb-specific parameters
        String csvOutputFilePath = temporaryFolder.newFile().getAbsolutePath();
        paramsMap.put(CsvWritingLdbcSnbInteractiveDb.CSV_PATH_KEY, csvOutputFilePath);
        // Driver-specific parameters
        String name = "name";
        String dbClassName = CsvWritingLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 100;
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
        boolean calculateWorkloadStatistics = false;
        Duration spinnerSleepDuration = Duration.fromMilli(0);
        boolean printHelp = false;

        ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(paramsMap, name, dbClassName, workloadClassName, operationCount,
                threadCount, statusDisplayInterval, timeUnit, resultDirPath, timeCompressionRatio, windowedExecutionWindowDuration, peerIds, toleratedExecutionDelay,
                validationParams, dbValidationFilePath, validateWorkload, calculateWorkloadStatistics, spinnerSleepDuration, printHelp);

        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init(configuration);

        List<Operation<?>> operations = Lists.newArrayList(workload.operations(new GeneratorFactory(new RandomDataGeneratorFactory(42L)), configuration.operationCount()));
        Time prevOperationScheduledStartTime = operations.get(0).scheduledStartTime().minus(Duration.fromMilli(1));
        for (Operation<?> operation : operations) {
            assertThat(operation.scheduledStartTime().gte(prevOperationScheduledStartTime), is(true));
            prevOperationScheduledStartTime = operation.scheduledStartTime();
        }

        List<Operation<?>> offsetOperations = Lists.newArrayList(gf.timeOffset(operations.iterator(), TIME_SOURCE.now().plus(Duration.fromMilli(500))));
        Time prevOffsetOperationScheduledStartTime = offsetOperations.get(0).scheduledStartTime().minus(Duration.fromMilli(1));
        for (Operation<?> operation : offsetOperations) {
            assertThat(operation.scheduledStartTime().gte(prevOffsetOperationScheduledStartTime), is(true));
            prevOffsetOperationScheduledStartTime = operation.scheduledStartTime();
        }
        workload.cleanup();
    }

    @Test
    public void shouldAlwaysProduceTheSameOutputWhenGivenTheSameInput() {
        // Given
        List<Operation<?>> operations = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
        );

        Time now = new SystemTimeSource().now();

        List<Operation<?>> offsetOperations1 = Lists.newArrayList(gf.timeOffset(operations.iterator(), now));
        List<Operation<?>> offsetOperations2 = Lists.newArrayList(gf.timeOffset(operations.iterator(), now));
        List<Operation<?>> offsetOperations3 = Lists.newArrayList(gf.timeOffset(operations.iterator(), now));
        List<Operation<?>> offsetOperations4 = Lists.newArrayList(gf.timeOffset(operations.iterator(), now));

        // When
        GeneratorFactory.OperationStreamComparisonResult stream1Stream2Comparison = gf.compareOperationStreams(offsetOperations1.iterator(), offsetOperations2.iterator(), true);
        GeneratorFactory.OperationStreamComparisonResult stream1Stream3Comparison = gf.compareOperationStreams(offsetOperations1.iterator(), offsetOperations3.iterator(), true);
        GeneratorFactory.OperationStreamComparisonResult stream1Stream4Comparison = gf.compareOperationStreams(offsetOperations1.iterator(), offsetOperations4.iterator(), true);
        GeneratorFactory.OperationStreamComparisonResult stream2Stream1Comparison = gf.compareOperationStreams(offsetOperations2.iterator(), offsetOperations1.iterator(), true);
        GeneratorFactory.OperationStreamComparisonResult stream2Stream3Comparison = gf.compareOperationStreams(offsetOperations2.iterator(), offsetOperations3.iterator(), true);
        GeneratorFactory.OperationStreamComparisonResult stream2Stream4Comparison = gf.compareOperationStreams(offsetOperations2.iterator(), offsetOperations4.iterator(), true);
        GeneratorFactory.OperationStreamComparisonResult stream3Stream1Comparison = gf.compareOperationStreams(offsetOperations3.iterator(), offsetOperations1.iterator(), true);
        GeneratorFactory.OperationStreamComparisonResult stream3Stream2Comparison = gf.compareOperationStreams(offsetOperations3.iterator(), offsetOperations2.iterator(), true);
        GeneratorFactory.OperationStreamComparisonResult stream3Stream4Comparison = gf.compareOperationStreams(offsetOperations3.iterator(), offsetOperations4.iterator(), true);
        GeneratorFactory.OperationStreamComparisonResult stream4Stream1Comparison = gf.compareOperationStreams(offsetOperations4.iterator(), offsetOperations1.iterator(), true);
        GeneratorFactory.OperationStreamComparisonResult stream4Stream2Comparison = gf.compareOperationStreams(offsetOperations4.iterator(), offsetOperations2.iterator(), true);
        GeneratorFactory.OperationStreamComparisonResult stream4Stream3Comparison = gf.compareOperationStreams(offsetOperations4.iterator(), offsetOperations3.iterator(), true);


        // Then
        assertThat(stream1Stream2Comparison.errorMessage(), stream1Stream2Comparison.resultType(), is(GeneratorFactory.OperationStreamComparisonResultType.PASS));
        assertThat(stream1Stream3Comparison.errorMessage(), stream1Stream3Comparison.resultType(), is(GeneratorFactory.OperationStreamComparisonResultType.PASS));
        assertThat(stream1Stream4Comparison.errorMessage(), stream1Stream4Comparison.resultType(), is(GeneratorFactory.OperationStreamComparisonResultType.PASS));
        assertThat(stream2Stream1Comparison.errorMessage(), stream2Stream1Comparison.resultType(), is(GeneratorFactory.OperationStreamComparisonResultType.PASS));
        assertThat(stream2Stream3Comparison.errorMessage(), stream2Stream3Comparison.resultType(), is(GeneratorFactory.OperationStreamComparisonResultType.PASS));
        assertThat(stream2Stream4Comparison.errorMessage(), stream2Stream4Comparison.resultType(), is(GeneratorFactory.OperationStreamComparisonResultType.PASS));
        assertThat(stream3Stream1Comparison.errorMessage(), stream3Stream1Comparison.resultType(), is(GeneratorFactory.OperationStreamComparisonResultType.PASS));
        assertThat(stream3Stream2Comparison.errorMessage(), stream3Stream2Comparison.resultType(), is(GeneratorFactory.OperationStreamComparisonResultType.PASS));
        assertThat(stream3Stream4Comparison.errorMessage(), stream3Stream4Comparison.resultType(), is(GeneratorFactory.OperationStreamComparisonResultType.PASS));
        assertThat(stream4Stream1Comparison.errorMessage(), stream4Stream1Comparison.resultType(), is(GeneratorFactory.OperationStreamComparisonResultType.PASS));
        assertThat(stream4Stream2Comparison.errorMessage(), stream4Stream2Comparison.resultType(), is(GeneratorFactory.OperationStreamComparisonResultType.PASS));
        assertThat(stream4Stream3Comparison.errorMessage(), stream4Stream3Comparison.resultType(), is(GeneratorFactory.OperationStreamComparisonResultType.PASS));
    }
}