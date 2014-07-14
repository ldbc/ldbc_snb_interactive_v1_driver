package com.ldbc.driver.runtime;

import com.ldbc.driver.*;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.coordination.NaiveSynchronizedConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.ThreadedQueuedConcurrentMetricsService;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.ManualTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyDb;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class GctAndSchedulingScenariosTest {
    @Ignore
    @Test
    public void test() throws DriverConfigurationException, DbException, CompletionTimeException, WorkloadException {
        TimeSource timeSource = new ManualTimeSource(0);
        Db db = new DummyDb();
        Map<String, String> params = new HashMap<>();
        params.put(DummyDb.SLEEP_DURATION_MILLI_ARG, "0");
        db.init(params);
        Iterator<Operation<?>> operations = null;//TODO
        Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications = null; // TODO
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ConcurrentMetricsService metricsService = new ThreadedQueuedConcurrentMetricsService(timeSource, errorReporter, TimeUnit.MILLISECONDS);
        ConcurrentCompletionTimeService completionTimeService = new NaiveSynchronizedConcurrentCompletionTimeService(new HashSet<String>());
        int threadCount = 1; // TODO
        Duration statusDisplayInterval = Duration.fromMilli(0);
        Time workloadStartTime = Time.fromMilli(0);
        // set very high so it never triggers a failure
        Duration toleratedExecutionDelayDuration = Duration.fromMinutes(100);
        Duration spinnerSleepDuration = Duration.fromMilli(0);
        // TODO need to be carefully selected for test
        Duration executionWindowDuration = Duration.fromMilli(100);
        WorkloadRunner runner = new WorkloadRunner(
                timeSource,
                db,
                operations,
                operationClassifications,
                metricsService,
                errorReporter,
                completionTimeService,
                threadCount,
                statusDisplayInterval,
                workloadStartTime,
                toleratedExecutionDelayDuration,
                spinnerSleepDuration,
                executionWindowDuration
        );

        assertThat(true, is(false));
    }
}
