package com.ldbc.driver.workload;

import com.ldbc.driver.BenchmarkPhase;
import com.ldbc.driver.ParamsException;
import com.ldbc.driver.WorkloadParams;
import com.ldbc.driver.util.TestUtils;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class WorkloadParamsTests {

    @Test
    public void shouldReturnSameAsConstructedWith() {
        Map<String, String> paramsMap = new HashMap<String, String>();
        String dbClassName = "dbClassName";
        String workloadClassName = "workloadClassName";
        long operationCount = 1;
        long recordCount = 2;
        BenchmarkPhase benchmarkPhase = BenchmarkPhase.LOAD_PHASE;
        int threadCount = 3;
        boolean showStatus = true;
        TimeUnit timeUnit = TimeUnit.SECONDS;
        String resultFilePath = null;

        WorkloadParams params = new WorkloadParams(paramsMap, dbClassName, workloadClassName, operationCount,
                recordCount, benchmarkPhase, threadCount, showStatus, timeUnit, resultFilePath);

        assertThat(params.asMap(), is(paramsMap));
        assertThat(params.dbClassName(), is(dbClassName));
        assertThat(params.workloadClassName(), is(workloadClassName));
        assertThat(params.operationCount(), is(operationCount));
        assertThat(params.recordCount(), is(recordCount));
        assertThat(params.benchmarkPhase(), is(benchmarkPhase));
        assertThat(params.threadCount(), is(threadCount));
        assertThat(params.isShowStatus(), is(showStatus));
        assertThat(params.timeUnit(), is(timeUnit));
        assertThat(params.resultFilePath(), is(resultFilePath));
    }

    @Test
    public void shouldReturnSameAsCommandline() throws ParamsException {
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
        long recordCount = 2;
        BenchmarkPhase benchmarkPhase = BenchmarkPhase.LOAD_PHASE;
        int threadCount = 3;
        boolean showStatus = true;
        String userKey = "userKey";
        String userVal = "userVal";
        TimeUnit timeUnit = TimeUnit.MINUTES;
        String resultFilePath = "somePath";

        String[] args = {"-db", dbClassName, "-w", workloadClassName, "-oc", Long.toString(operationCount), "-rc",
                Long.toString(recordCount), (benchmarkPhase.equals(BenchmarkPhase.LOAD_PHASE)) ? "-l" : "-t",
                "-tc", Integer.toString(threadCount), "-rf", resultFilePath, (showStatus) ? "-s" : "", "-p", userKey, userVal, "-tu",
                timeUnit.toString()};

        WorkloadParams params = WorkloadParams.fromArgs(args);

        assertThat(params.dbClassName(), is(dbClassName));
        assertThat(params.workloadClassName(), is(workloadClassName));
        assertThat(params.operationCount(), is(operationCount));
        assertThat(params.recordCount(), is(recordCount));
        assertThat(params.benchmarkPhase(), is(benchmarkPhase));
        assertThat(params.threadCount(), is(threadCount));
        assertThat(params.isShowStatus(), is(showStatus));
        assertThat(params.timeUnit(), is(timeUnit));
        assertThat(params.resultFilePath(), is(resultFilePath));
        assertThat(params.asMap().get(userKey), is(userVal));
    }

    @Test
    public void shouldReturnSameAsPropertiesFile() throws ParamsException {
        String[] args = {"-P", TestUtils.getResource("/test_workload_params.properties").getAbsolutePath()};
        WorkloadParams params = WorkloadParams.fromArgs(args);

        assertThat(params.dbClassName(), is("com.ldbc.socialnet.workload.neo4j.Neo4jDb"));
        assertThat(params.workloadClassName(), is("com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcInteractiveWorkload"));
        assertThat(params.operationCount(), is(10L));
        assertThat(params.recordCount(), is(-1L));
        assertThat(params.benchmarkPhase(), is(BenchmarkPhase.TRANSACTION_PHASE));
        assertThat(params.threadCount(), is(1));
        assertThat(params.isShowStatus(), is(true));
        assertThat(params.timeUnit(), is(TimeUnit.MILLISECONDS));
        assertThat(params.resultFilePath(), is("test_results.json"));
        assertThat(params.asMap().get("parameters"), is("ldbc_driver/workloads/ldbc/socnet/interactive/parameters.json"));
    }
}