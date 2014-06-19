package com.ldbc.driver.control;

import com.google.common.collect.Lists;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.util.TestUtils;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ConsoleAndFileDriverConfigurationTests {

    @Test
    public void shouldReturnSameAsConstructedWith() {
        Map<String, String> paramsMap = new HashMap<String, String>();
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
                calculateWorkloadStatistics);

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
        String peerIdsString0 = ConsoleAndFileDriverConfiguration.serializePeerIds(peerIds0);
        String peerIdsString1 = ConsoleAndFileDriverConfiguration.serializePeerIds(peerIds1);
        String peerIdsString2 = ConsoleAndFileDriverConfiguration.serializePeerIds(peerIds2);

        // Then
        assertThat(ConsoleAndFileDriverConfiguration.parsePeerIds(peerIdsString0), equalTo(peerIds0));
        assertThat(ConsoleAndFileDriverConfiguration.parsePeerIds(peerIdsString1), equalTo(peerIds1));
        assertThat(ConsoleAndFileDriverConfiguration.parsePeerIds(peerIdsString2), equalTo(peerIds2));
    }
}