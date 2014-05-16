package com.ldbc.driver.workloads;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.*;
import com.ldbc.driver.control.ConcurrentControlService;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.control.LocalControlService;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.streams.IteratorSplitter;
import com.ldbc.driver.runtime.streams.IteratorSplittingException;
import com.ldbc.driver.runtime.streams.SplitDefinition;
import com.ldbc.driver.runtime.streams.SplitResult;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.Bucket;
import com.ldbc.driver.util.Histogram;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import com.ldbc.driver.util.TestUtils;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.*;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.db.CsvDb;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.db.NothingDb;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class LdbcWorkloadTests {

    @Test
    public void shouldBeRepeatableWhenSameWorkloadIsUsedTwiceWithIdenticalGeneratorFactories() throws ClientException, DriverConfigurationException, WorkloadException {
        String ldbcSocNetInteractivePropertiesPath = TestUtils.getResource("/ldbc_socnet_interactive_test.properties").getAbsolutePath();
        String ldbcDriverPropertiesPath = TestUtils.getResource("/ldbc_driver_default_test.properties").getAbsolutePath();

        ConsoleAndFileDriverConfiguration params = ConsoleAndFileDriverConfiguration.fromArgs(new String[]{
                "-P", ldbcSocNetInteractivePropertiesPath,
                "-P", ldbcDriverPropertiesPath,
                // database class is loaded by Client class, which is bypassed in this test
                "-db", "this will never be used",
                "-oc", "100",
                "-p", "neo4j.dbtype", "embedded-cypher",
                "-rf", "report/result-embedded-cypher.json"});

        Workload workload = new LdbcInteractiveWorkload();
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
        String ldbcSocNetInteractivePropertiesPath = TestUtils.getResource("/ldbc_socnet_interactive_test.properties").getAbsolutePath();
        String ldbcDriverPropertiesPath = TestUtils.getResource("/ldbc_driver_default_test.properties").getAbsolutePath();

        ConsoleAndFileDriverConfiguration params = ConsoleAndFileDriverConfiguration.fromArgs(new String[]{
                "-P", ldbcSocNetInteractivePropertiesPath,
                "-P", ldbcDriverPropertiesPath,
                // database class is loaded by Client class, which is bypassed in this test
                "-db", "this will never be used",
                "-oc", "100",
                "-p", "neo4j.dbtype", "embedded-api-steps",
                "-rf", "report/result-embedded-steps.json"});

        Workload workloadA = new LdbcInteractiveWorkload();
        workloadA.init(params);

        Workload workloadB = new LdbcInteractiveWorkload();
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
    public void shouldGenerateConfiguredQueryMix() throws ClientException, DriverConfigurationException, WorkloadException {
        // Given

        String ldbcSocNetInteractivePropertiesPath = TestUtils.getResource("/ldbc_socnet_interactive_test.properties").getAbsolutePath();
        String ldbcDriverPropertiesPath = TestUtils.getResource("/ldbc_driver_default_test.properties").getAbsolutePath();

        ConsoleAndFileDriverConfiguration params = ConsoleAndFileDriverConfiguration.fromArgs(new String[]{
                "-P", ldbcSocNetInteractivePropertiesPath,
                "-P", ldbcDriverPropertiesPath,
                // database class is loaded by Client class, which is bypassed in this test
                "-db", "this will never be used",
                "-oc", "10000",
                "-p", LdbcQuery1.class.getName(), "1",
                "-p", LdbcQuery2.class.getName(), "2",
                "-p", LdbcQuery3.class.getName(), "3",
                "-p", LdbcQuery4.class.getName(), "4",
                "-p", LdbcQuery5.class.getName(), "5",
                "-p", LdbcQuery6.class.getName(), "6",
                "-p", LdbcQuery7.class.getName(), "7",
                "-p", LdbcQuery8.class.getName(), "6",
                "-p", LdbcQuery9.class.getName(), "5",
                "-p", LdbcQuery10.class.getName(), "4",
                "-p", LdbcQuery11.class.getName(), "3",
                "-p", LdbcQuery12.class.getName(), "2",
                "-p", LdbcQuery13.class.getName(), "1",
                "-p", LdbcQuery14.class.getName(), "1"
        });

        Workload workload = new LdbcInteractiveWorkload();
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

        Histogram<Class, Long> expectedQueryMixHistogram = new Histogram<Class, Long>(0l);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery1.class), 1l);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery2.class), 2l);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery3.class), 3l);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery4.class), 4l);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery5.class), 5l);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery6.class), 6l);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery7.class), 7l);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery8.class), 6l);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery9.class), 5l);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery10.class), 4l);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery11.class), 3l);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery12.class), 2l);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery13.class), 1l);
        expectedQueryMixHistogram.addBucket(Bucket.DiscreteBucket.create((Class) LdbcQuery14.class), 1l);

        Histogram<Class, Long> actualQueryMixHistogram = new Histogram<Class, Long>(0l);
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
                String.format("Distributions should be within tolerance: %s", tolerance),
                Histogram.equalsWithinTolerance(
                        actualQueryMixHistogram.toPercentageValues(),
                        expectedQueryMixHistogram.toPercentageValues(),
                        tolerance),
                is(true));
    }

    @Test
    public void shouldGenerateConfiguredQueryMixAndWriteToCsv() throws ClientException, DriverConfigurationException, WorkloadException, IOException {
        // Given
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
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_1_KEY, "true");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_2_KEY, "true");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_3_KEY, "true");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_4_KEY, "true");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_5_KEY, "true");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_6_KEY, "true");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_7_KEY, "true");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_8_KEY, "true");
        paramsMap.put(LdbcInteractiveWorkload.PARAMETERS_FILENAME_KEY, "ldbc_driver/workloads/ldbc/socnet/interactive/parameters.json");
        paramsMap.put(LdbcInteractiveWorkload.INTERLEAVE_DURATION_KEY, "1");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_STREAM_FILENAME_KEY, "ldbc_driver/workloads/ldbc/socnet/interactive/updates.csv");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_RATIO_KEY, "1");
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

        ConsoleAndFileDriverConfiguration params = new ConsoleAndFileDriverConfiguration(paramsMap, dbClassName, workloadClassName, operationCount,
                threadCount, showStatus, timeUnit, resultFilePath, timeCompressionRatio, gctDeltaDuration, peerIds, toleratedExecutionDelay);

        // When
        Client client = new Client(new LocalControlService(Time.now().plus(Duration.fromMilli(500)), params));
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
        String ldbcSocnetInteractiveTestPropertiesPath =
                new File("ldbc_driver/workloads/ldbc/socnet/interactive/ldbc_socnet_interactive.properties").getAbsolutePath();
        String ldbcDriverTestPropertiesPath =
                new File("ldbc_driver/src/main/resources/ldbc_driver_default.properties").getAbsolutePath();

        String csvOutputFilePath = "temp_csv_output_file.csv";
        FileUtils.deleteQuietly(new File(csvOutputFilePath));
        String resultFilePath = "test_write_to_csv_results.json";

        assertThat(new File(csvOutputFilePath).exists(), is(false));
        assertThat(new File(resultFilePath).exists(), is(false));

        assertThat(new File(ldbcSocnetInteractiveTestPropertiesPath).exists(), is(true));
        assertThat(new File(ldbcDriverTestPropertiesPath).exists(), is(true));

        ConsoleAndFileDriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromArgs(new String[]{
                "-" + ConsoleAndFileDriverConfiguration.RESULT_FILE_PATH_ARG, resultFilePath,
                "-" + ConsoleAndFileDriverConfiguration.DB_ARG, CsvDb.class.getName(),
                "-p", CsvDb.CSV_PATH_KEY, csvOutputFilePath,
                "-P", ldbcSocnetInteractiveTestPropertiesPath,
                "-P", ldbcDriverTestPropertiesPath});


        assertThat(new File(csvOutputFilePath).exists(), is(false));
        assertThat(new File(resultFilePath).exists(), is(false));


        // When
        Client client = new Client(new LocalControlService(Time.now().plus(Duration.fromMilli(500)), configuration));
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
    public void shouldAssignMonotonicallyIncreasingScheduledStartTimesToOperations() throws WorkloadException {
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
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_1_KEY, "true");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_2_KEY, "true");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_3_KEY, "true");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_4_KEY, "true");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_5_KEY, "true");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_6_KEY, "true");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_7_KEY, "true");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_8_KEY, "true");
        paramsMap.put(LdbcInteractiveWorkload.PARAMETERS_FILENAME_KEY, "ldbc_driver/workloads/ldbc/socnet/interactive/parameters.json");
        paramsMap.put(LdbcInteractiveWorkload.INTERLEAVE_DURATION_KEY, "10");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_STREAM_FILENAME_KEY, "ldbc_driver/workloads/ldbc/socnet/interactive/updates.csv");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_RATIO_KEY, "1");
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

        ConcurrentControlService controlService = new LocalControlService(Time.now().plus(Duration.fromMilli(500)), configuration);
        Workload workload = new LdbcInteractiveWorkload();
        workload.init(configuration);
        List<Operation<?>> operations = Lists.newArrayList(workload.operations(new GeneratorFactory(new RandomDataGeneratorFactory(42L))));

        Time prevOperationScheduledStartTime = operations.get(0).scheduledStartTime().minus(Duration.fromMilli(1));
        for (Operation<?> operation : Lists.newArrayList(operations)) {
            assertThat(operation.scheduledStartTime().gt(prevOperationScheduledStartTime), is(true));
            prevOperationScheduledStartTime = operation.scheduledStartTime();
        }
    }

    @Test
    public void operationsShouldHaveMonotonicallyIncreasingScheduledStartTimesAfterSplitting() throws WorkloadException {
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
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_1_KEY, "true");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_2_KEY, "true");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_3_KEY, "true");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_4_KEY, "true");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_5_KEY, "true");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_6_KEY, "true");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_7_KEY, "true");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_8_KEY, "true");
        paramsMap.put(LdbcInteractiveWorkload.PARAMETERS_FILENAME_KEY, "ldbc_driver/workloads/ldbc/socnet/interactive/parameters.json");
        paramsMap.put(LdbcInteractiveWorkload.INTERLEAVE_DURATION_KEY, "10");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_STREAM_FILENAME_KEY, "ldbc_driver/workloads/ldbc/socnet/interactive/updates.csv");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_RATIO_KEY, "1");
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

        Time firstOperationScheduledStartTime = operations.get(0).scheduledStartTime();

        Time prevOperationScheduledStartTime = firstOperationScheduledStartTime.minus(Duration.fromMilli(1));
        for (Operation<?> operation : Lists.newArrayList(operations)) {
            assertThat(operation.scheduledStartTime().gt(prevOperationScheduledStartTime), is(true));
            prevOperationScheduledStartTime = operation.scheduledStartTime();
        }

        List<Operation<?>> windowedOperations;
        List<Operation<?>> blockingOperations;
        List<Operation<?>> asynchronousOperations;
        try {
            IteratorSplitter<Operation<?>> splitter = new IteratorSplitter<Operation<?>>(IteratorSplitter.UnmappedItemPolicy.ABORT);
            SplitDefinition<Operation<?>> windowed = new SplitDefinition<Operation<?>>(Workload.operationTypesBySchedulingMode(workload.operationClassifications(), OperationClassification.SchedulingMode.WINDOWED));
            SplitDefinition<Operation<?>> blocking = new SplitDefinition<Operation<?>>(Workload.operationTypesBySchedulingMode(workload.operationClassifications(), OperationClassification.SchedulingMode.INDIVIDUAL_BLOCKING));
            SplitDefinition<Operation<?>> asynchronous = new SplitDefinition<Operation<?>>(Workload.operationTypesBySchedulingMode(workload.operationClassifications(), OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC));
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
            assertThat(operation.scheduledStartTime().gt(prevWindowedOperationScheduledStartTime), is(true));
            prevWindowedOperationScheduledStartTime = operation.scheduledStartTime();
        }

        Time prevAsyncOperationScheduledStartTime = firstOperationScheduledStartTime.minus(Duration.fromMilli(1));
        for (Operation<?> operation : asynchronousOperations) {
            assertThat(operation.scheduledStartTime().gt(prevAsyncOperationScheduledStartTime), is(true));
            prevAsyncOperationScheduledStartTime = operation.scheduledStartTime();
        }

        Time prevBlockingOperationScheduledStartTime = firstOperationScheduledStartTime.minus(Duration.fromMilli(1));
        for (Operation<?> operation : blockingOperations) {
            assertThat(operation.scheduledStartTime().gt(prevBlockingOperationScheduledStartTime), is(true));
            prevBlockingOperationScheduledStartTime = operation.scheduledStartTime();
        }
    }

    @Test
    public void shouldNotFailUnexpectedlyWhenQueriesAreLongRunning() throws ClientException, DriverConfigurationException, WorkloadException, IOException {
        // Given
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
        paramsMap.put(LdbcInteractiveWorkload.INTERLEAVE_DURATION_KEY, "1");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_STREAM_FILENAME_KEY, "ldbc_driver/workloads/ldbc/socnet/interactive/updates.csv");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_RATIO_KEY, "0");
        paramsMap.put(LdbcInteractiveWorkload.READ_RATIO_KEY, "1");
        // NothingDb-specific parameters
        paramsMap.put(NothingDb.SLEEP_DURATION_MILLI, Long.toString(Duration.fromSeconds(40).asMilli()));
        // Driver-specific parameters
        String dbClassName = NothingDb.class.getName();
        String workloadClassName = LdbcInteractiveWorkload.class.getName();
        long operationCount = 5;
        int threadCount = 2;
        boolean showStatus = true;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultFilePath = "test_write_to_csv_results.json";
        FileUtils.deleteQuietly(new File(resultFilePath));
        Double timeCompressionRatio = null;
        Duration gctDeltaDuration = Duration.fromSeconds(10);
        List<String> peerIds = Lists.newArrayList();
        Duration toleratedExecutionDelay = Duration.fromMinutes(5);

        assertThat(new File(resultFilePath).exists(), is(false));

        ConsoleAndFileDriverConfiguration params = new ConsoleAndFileDriverConfiguration(paramsMap, dbClassName, workloadClassName, operationCount,
                threadCount, showStatus, timeUnit, resultFilePath, timeCompressionRatio, gctDeltaDuration, peerIds, toleratedExecutionDelay);

        // When
        Client client = new Client(new LocalControlService(Time.now().plus(Duration.fromMilli(500)), params));
        client.start();

        // Then
        assertThat(new File(resultFilePath).exists(), is(true));
        FileUtils.deleteQuietly(new File(resultFilePath));
        assertThat(new File(resultFilePath).exists(), is(false));
    }
}
