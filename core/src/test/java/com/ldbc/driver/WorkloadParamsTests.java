package com.ldbc.driver;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class WorkloadParamsTests
{

    enum Cows
    {
        Black
    }

    @Test
    public void shouldReturnSameAsConstructedWith()
    {
        Map<String, String> paramsMap = new HashMap<String, String>();
        String dbClassName = "dbClassName";
        String workloadClassName = "workloadClassName";
        long operationCount = 1;
        long recordCount = 2;
        BenchmarkPhase benchmarkPhase = BenchmarkPhase.LOAD_PHASE;
        int threadCount = 3;
        boolean showStatus = true;
        TimeUnit timeUnit = TimeUnit.SECONDS;

        WorkloadParams params = new WorkloadParams( paramsMap, dbClassName, workloadClassName, operationCount,
                recordCount, benchmarkPhase, threadCount, showStatus, timeUnit );

        assertThat( params.asMap(), is( paramsMap ) );
        assertThat( params.getDbClassName(), is( dbClassName ) );
        assertThat( params.getWorkloadClassName(), is( workloadClassName ) );
        assertThat( params.getOperationCount(), is( operationCount ) );
        assertThat( params.getRecordCount(), is( recordCount ) );
        assertThat( params.getBenchmarkPhase(), is( benchmarkPhase ) );
        assertThat( params.getThreadCount(), is( threadCount ) );
        assertThat( params.isShowStatus(), is( showStatus ) );
        assertThat( params.getTimeUnit(), is( timeUnit ) );
    }

    @Test
    public void shouldResultSameAsCommandline() throws ParamsException
    {
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

        String[] args = { "-db", dbClassName, "-w", workloadClassName, "-oc", Long.toString( operationCount ), "-rc",
                Long.toString( recordCount ), ( benchmarkPhase.equals( BenchmarkPhase.LOAD_PHASE ) ) ? "-l" : "-t",
                "-tc", Integer.toString( threadCount ), ( showStatus ) ? "-s" : "", "-p", userKey, userVal, "-tu",
                timeUnit.toString(), "-x" };

        WorkloadParams params = WorkloadParams.fromArgs( args );

        assertThat( params.getDbClassName(), is( dbClassName ) );
        assertThat( params.getWorkloadClassName(), is( workloadClassName ) );
        assertThat( params.getOperationCount(), is( operationCount ) );
        assertThat( params.getRecordCount(), is( recordCount ) );
        assertThat( params.getBenchmarkPhase(), is( benchmarkPhase ) );
        assertThat( params.getThreadCount(), is( threadCount ) );
        assertThat( params.isShowStatus(), is( showStatus ) );
        assertThat( params.getTimeUnit(), is( timeUnit ) );
        assertThat( (String) params.asMap().get( userKey ), is( userVal ) );
    }
}
