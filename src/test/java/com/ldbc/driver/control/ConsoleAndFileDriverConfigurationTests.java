package com.ldbc.driver.control;

import com.google.common.collect.Lists;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.util.TestUtils;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ConsoleAndFileDriverConfigurationTests {

    @Test
    public void shouldReturnSameConfigurationFromFromArgsAsFromFromParamsMap() throws DriverConfigurationException {
        List<String> peerIds = Lists.newArrayList("peerId1", "peerId2");

        Map<String, String> requiredParamsMap = new HashMap<>();
        requiredParamsMap.put(ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, "1");
        requiredParamsMap.put(ConsoleAndFileDriverConfiguration.WORKLOAD_ARG, "workload class name");
        requiredParamsMap.put(ConsoleAndFileDriverConfiguration.DB_ARG, "db class name");

        Map<String, String> optionalParamsMap = new HashMap<>();
        optionalParamsMap.put(ConsoleAndFileDriverConfiguration.THREADS_ARG, ConsoleAndFileDriverConfiguration.THREADS_DEFAULT_STRING);
        optionalParamsMap.put(ConsoleAndFileDriverConfiguration.SHOW_STATUS_ARG, ConsoleAndFileDriverConfiguration.SHOW_STATUS_DEFAULT_STRING);
        optionalParamsMap.put(ConsoleAndFileDriverConfiguration.TIME_UNIT_ARG, ConsoleAndFileDriverConfiguration.TIME_UNIT_DEFAULT_STRING);
        optionalParamsMap.put(ConsoleAndFileDriverConfiguration.RESULT_FILE_PATH_ARG, ConsoleAndFileDriverConfiguration.RESULT_FILE_PATH_DEFAULT_STRING);
        optionalParamsMap.put(ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG, ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_DEFAULT_STRING);
        optionalParamsMap.put(ConsoleAndFileDriverConfiguration.GCT_DELTA_DURATION_ARG, ConsoleAndFileDriverConfiguration.GCT_DELTA_DURATION_DEFAULT_STRING);
        optionalParamsMap.put(ConsoleAndFileDriverConfiguration.PEER_IDS_ARG, ConsoleAndFileDriverConfiguration.serializePeerIdsToJson(peerIds));
        optionalParamsMap.put(ConsoleAndFileDriverConfiguration.TOLERATED_EXECUTION_DELAY_ARG, ConsoleAndFileDriverConfiguration.TOLERATED_EXECUTION_DELAY_DEFAULT_STRING);
        optionalParamsMap.put(ConsoleAndFileDriverConfiguration.VALIDATE_DB_ARG, ConsoleAndFileDriverConfiguration.VALIDATE_DB_DEFAULT_STRING);
        optionalParamsMap.put(ConsoleAndFileDriverConfiguration.VALIDATE_WORKLOAD_ARG, ConsoleAndFileDriverConfiguration.VALIDATE_WORKLOAD_DEFAULT_STRING);
        optionalParamsMap.put(ConsoleAndFileDriverConfiguration.CALCULATE_WORKLOAD_STATISTICS_ARG, ConsoleAndFileDriverConfiguration.CALCULATE_WORKLOAD_STATISTICS_DEFAULT_STRING);
        optionalParamsMap.put(ConsoleAndFileDriverConfiguration.SPINNER_SLEEP_DURATION_ARG, ConsoleAndFileDriverConfiguration.SPINNER_SLEEP_DURATION_DEFAULT_STRING);

        List<String> requiredParamsArgsList = new ArrayList<>();
        requiredParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, "1"));
        requiredParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.WORKLOAD_ARG, "workload class name"));
        requiredParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.DB_ARG, "db class name"));

        List<String> optionalParamsArgsList = new ArrayList<>();
        optionalParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.THREADS_ARG, ConsoleAndFileDriverConfiguration.THREADS_DEFAULT_STRING));
        if (ConsoleAndFileDriverConfiguration.SHOW_STATUS_DEFAULT)
            optionalParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.SHOW_STATUS_ARG));
        optionalParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.TIME_UNIT_ARG, ConsoleAndFileDriverConfiguration.TIME_UNIT_DEFAULT_STRING));
        if (null != ConsoleAndFileDriverConfiguration.RESULT_FILE_PATH_DEFAULT)
            optionalParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.RESULT_FILE_PATH_ARG, ConsoleAndFileDriverConfiguration.RESULT_FILE_PATH_DEFAULT_STRING));
        optionalParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG, ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_DEFAULT_STRING));
        optionalParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.GCT_DELTA_DURATION_ARG, ConsoleAndFileDriverConfiguration.GCT_DELTA_DURATION_DEFAULT_STRING));
        optionalParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.PEER_IDS_ARG, ConsoleAndFileDriverConfiguration.serializePeerIdsToCommandline(peerIds)));
        optionalParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.TOLERATED_EXECUTION_DELAY_ARG, ConsoleAndFileDriverConfiguration.TOLERATED_EXECUTION_DELAY_DEFAULT_STRING));
        if (ConsoleAndFileDriverConfiguration.VALIDATE_DB_DEFAULT)
            optionalParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.VALIDATE_DB_ARG));
        if (ConsoleAndFileDriverConfiguration.VALIDATE_WORKLOAD_DEFAULT)
            optionalParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.VALIDATE_WORKLOAD_ARG));
        if (ConsoleAndFileDriverConfiguration.CALCULATE_WORKLOAD_STATISTICS_DEFAULT)
            optionalParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.CALCULATE_WORKLOAD_STATISTICS_ARG));
        optionalParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.SPINNER_SLEEP_DURATION_ARG, ConsoleAndFileDriverConfiguration.SPINNER_SLEEP_DURATION_DEFAULT_STRING));

        // When
        Map<String, String> paramsMap = MapUtils.mergeMaps(requiredParamsMap, optionalParamsMap, false);
        DriverConfiguration configurationFromParamsMap = ConsoleAndFileDriverConfiguration.fromParamsMap(paramsMap);

        String[] requiredParamsArgs = requiredParamsArgsList.toArray(new String[requiredParamsArgsList.size()]);
        String[] optionalParamsArgs = optionalParamsArgsList.toArray(new String[optionalParamsArgsList.size()]);
        System.arraycopy(requiredParamsArgs, 0, optionalParamsArgs, 0, requiredParamsArgs.length);
        DriverConfiguration configurationFromParamsArgs = ConsoleAndFileDriverConfiguration.fromArgs(optionalParamsArgs);

        // Then
        assertThat(configurationFromParamsMap, equalTo(configurationFromParamsArgs));
    }

    @Test
    public void shouldWorkWhenOnlyRequiredParametersAreGiven() throws DriverConfigurationException {
        // Given
        Map<String, String> requiredParams = new HashMap<>();
        requiredParams.put(ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, "1");
        requiredParams.put(ConsoleAndFileDriverConfiguration.WORKLOAD_ARG, "workload class name");
        requiredParams.put(ConsoleAndFileDriverConfiguration.DB_ARG, "db class name");

        // When
        ConsoleAndFileDriverConfiguration configurationFromParams = ConsoleAndFileDriverConfiguration.fromParamsMap(requiredParams);

        // Then
        assertThat(configurationFromParams.dbClassName(), equalTo("db class name"));
        assertThat(configurationFromParams.workloadClassName(), equalTo("workload class name"));
        assertThat(configurationFromParams.operationCount(), is(1l));
        assertThat(configurationFromParams.threadCount(), is(ConsoleAndFileDriverConfiguration.THREADS_DEFAULT));
        assertThat(configurationFromParams.showStatus(), is(ConsoleAndFileDriverConfiguration.SHOW_STATUS_DEFAULT));
        assertThat(configurationFromParams.timeUnit(), is(ConsoleAndFileDriverConfiguration.TIME_UNIT_DEFAULT));
        assertThat(configurationFromParams.resultFilePath(), is(ConsoleAndFileDriverConfiguration.RESULT_FILE_PATH_DEFAULT));
        assertThat(configurationFromParams.timeCompressionRatio(), is(ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_DEFAULT));
        assertThat(configurationFromParams.gctDeltaDuration(), is(ConsoleAndFileDriverConfiguration.GCT_DELTA_DURATION_DEFAULT));
        assertThat(configurationFromParams.peerIds(), is(ConsoleAndFileDriverConfiguration.PEER_IDS_DEFAULT));
        assertThat(configurationFromParams.toleratedExecutionDelay(), is(ConsoleAndFileDriverConfiguration.TOLERATED_EXECUTION_DELAY_DEFAULT));
        assertThat(configurationFromParams.validateDatabase(), is(ConsoleAndFileDriverConfiguration.VALIDATE_DB_DEFAULT));
        assertThat(configurationFromParams.validateWorkload(), is(ConsoleAndFileDriverConfiguration.VALIDATE_WORKLOAD_DEFAULT));
        assertThat(configurationFromParams.calculateWorkloadStatistics(), is(ConsoleAndFileDriverConfiguration.CALCULATE_WORKLOAD_STATISTICS_DEFAULT));
        assertThat(configurationFromParams.compressedGctDeltaDuration(),
                is(Duration.fromMilli(Math.round(ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_DEFAULT * ConsoleAndFileDriverConfiguration.GCT_DELTA_DURATION_DEFAULT.asMilli()))));
        assertThat(configurationFromParams.spinnerSleepDuration(), is(ConsoleAndFileDriverConfiguration.SPINNER_SLEEP_DURATION_DEFAULT));
    }

    @Test
    public void shouldReturnSameAsConstructedWith() {
        Map<String, String> paramsMap = new HashMap<>();
        String dbClassName = "dbClassName";
        String workloadClassName = "workloadClassName";
        long operationCount = 1;
        int threadCount = 3;
        boolean showStatus = true;
        TimeUnit timeUnit = TimeUnit.SECONDS;
        String resultFilePath = null;
        Double timeCompressionRatio = 1.0;
        Duration gctDeltaDuration = Duration.fromMilli(1);
        List<String> peerIds = Lists.newArrayList("1");
        Duration toleratedExecutionDelay = Duration.fromMilli(2);
        boolean validateDatabase = false;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = false;
        Duration spinnerSleepDuration = Duration.fromMilli(0);

        ConsoleAndFileDriverConfiguration params = new ConsoleAndFileDriverConfiguration(
                paramsMap,
                dbClassName,
                workloadClassName,
                operationCount,
                threadCount,
                showStatus,
                timeUnit,
                resultFilePath,
                timeCompressionRatio,
                gctDeltaDuration,
                peerIds,
                toleratedExecutionDelay,
                validateDatabase,
                validateWorkload,
                calculateWorkloadStatistics,
                spinnerSleepDuration);

        assertThat(params.asMap(), equalTo(paramsMap));
        assertThat(params.dbClassName(), equalTo(dbClassName));
        assertThat(params.workloadClassName(), equalTo(workloadClassName));
        assertThat(params.operationCount(), equalTo(operationCount));
        assertThat(params.threadCount(), equalTo(threadCount));
        assertThat(params.showStatus(), equalTo(showStatus));
        assertThat(params.timeUnit(), equalTo(timeUnit));
        assertThat(params.resultFilePath(), equalTo(resultFilePath));
        assertThat(params.timeCompressionRatio(), equalTo(timeCompressionRatio));
        assertThat(params.gctDeltaDuration(), equalTo(gctDeltaDuration));
        assertThat(params.peerIds(), equalTo(peerIds));
        assertThat(params.toleratedExecutionDelay(), equalTo(toleratedExecutionDelay));
        assertThat(params.validateDatabase(), equalTo(validateDatabase));
        assertThat(params.validateWorkload(), equalTo(validateWorkload));
        assertThat(params.calculateWorkloadStatistics(), equalTo(calculateWorkloadStatistics));
        assertThat(params.spinnerSleepDuration(), equalTo(spinnerSleepDuration));
    }

    @Test
    public void shouldReturnSameAsCommandline() throws DriverConfigurationException {
        /*
        java -cp core-0.1-SNAPSHOT.jar com.ldbc.driver.Client -db <classname> -l | -t -oc <count> [-P
           <file1:file2>] [-p <key=value>] -rc <count> [-s]  [-tc <count>] -w <classname>
           
        -db,--database <classname>       classname of the DB to use (e.g. com.ldbc.driver.db.basic.BasicDb)
        -l,--load                        run the loading phase of the workload
        -oc,--operationcount <count>     number of operations to execute (default: 0)
        -P <file1:file2>                 load properties from file(s) - files will be loaded in the order provided
        -p <key=value>                   properties to be passed to DB and Workload - these will override
                                        properties loaded from files
        -rc,--recordcount <count>        number of records to create during load phase (default: 0)
        -s,--status                      show status during run
        -t,--transaction                 run the transactions phase of the workload
        -tc,--threadcount <count>        number of worker threads to execute with (default: CPU cores - 2)
        -w,--workload <classname>        classname of the Workload to use (e.g.
         */
        String dbClassName = "dbClassName";
        String workloadClassName = "workloadClassName";
        long operationCount = 1;
        int threadCount = 3;
        boolean showStatus = true;
        String userKey = "userKey";
        String userVal = "userVal";
        TimeUnit timeUnit = TimeUnit.MINUTES;
        String resultFilePath = "somePath";

        String[] args = {
                "-db", dbClassName,
                "-w", workloadClassName,
                "-oc", Long.toString(operationCount),
                "-tc", Integer.toString(threadCount),
                "-rf", resultFilePath,
                (showStatus) ? "-s" : "",
                "-p", userKey, userVal,
                "-tu", timeUnit.toString()};

        ConsoleAndFileDriverConfiguration params = ConsoleAndFileDriverConfiguration.fromArgs(args);

        assertThat(params.dbClassName(), is(dbClassName));
        assertThat(params.workloadClassName(), is(workloadClassName));
        assertThat(params.operationCount(), is(operationCount));
        assertThat(params.threadCount(), is(threadCount));
        assertThat(params.showStatus(), is(showStatus));
        assertThat(params.timeUnit(), is(timeUnit));
        assertThat(params.resultFilePath(), is(new File(resultFilePath).getAbsolutePath()));
        assertThat(params.asMap().get(userKey), is(userVal));
    }

    @Test
    public void shouldReturnSameAsPropertiesFile() throws DriverConfigurationException {
        String ldbcSocNetInteractivePropertiesPath = TestUtils.getResource("/ldbc_socnet_interactive_test.properties").getAbsolutePath();
        String ldbcDriverPropertiesPath = TestUtils.getResource("/ldbc_driver_default_test.properties").getAbsolutePath();

        ConsoleAndFileDriverConfiguration params = ConsoleAndFileDriverConfiguration.fromArgs(new String[]{
                "-P", ldbcSocNetInteractivePropertiesPath,
                "-P", ldbcDriverPropertiesPath,
                "-db", "com.ldbc.socialnet.workload.neo4j.Neo4jDb"
        });

        assertThat(params.dbClassName(), is("com.ldbc.socialnet.workload.neo4j.Neo4jDb"));
        assertThat(params.workloadClassName(), is("com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcInteractiveWorkload"));
        assertThat(params.operationCount(), is(10L));
        assertThat(params.threadCount(), is(1));
        assertThat(params.showStatus(), is(true));
        assertThat(params.timeUnit(), is(TimeUnit.MILLISECONDS));
        assertThat(params.resultFilePath(), is(new File("test_ldbc_socnet_interactive_results.json").getAbsolutePath()));
    }

    @Test
    public void shouldSerializeAndParsePeerIds() throws DriverConfigurationException {
        // Given
        List<String> peerIds0 = Lists.newArrayList();
        List<String> peerIds1 = Lists.newArrayList("1", "2");
        List<String> peerIds2 = Lists.newArrayList("1", "2", "cows");

        // When
        String peerIdsString0 = ConsoleAndFileDriverConfiguration.serializePeerIdsToJson(peerIds0);
        String peerIdsString1 = ConsoleAndFileDriverConfiguration.serializePeerIdsToJson(peerIds1);
        String peerIdsString2 = ConsoleAndFileDriverConfiguration.serializePeerIdsToJson(peerIds2);

        // Then
        assertThat(ConsoleAndFileDriverConfiguration.parsePeerIdsFromJson(peerIdsString0), equalTo(peerIds0));
        assertThat(ConsoleAndFileDriverConfiguration.parsePeerIdsFromJson(peerIdsString1), equalTo(peerIds1));
        assertThat(ConsoleAndFileDriverConfiguration.parsePeerIdsFromJson(peerIdsString2), equalTo(peerIds2));
    }
}