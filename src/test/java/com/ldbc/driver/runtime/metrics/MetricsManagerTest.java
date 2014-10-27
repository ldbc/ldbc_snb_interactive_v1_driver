package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.OperationResultReportTestHelper;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveOperationInstances;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class MetricsManagerTest {
    private final TemporalUtil temporalUtil = new TemporalUtil();
    private final TimeSource timeSource = new SystemTimeSource();
    private final boolean recordStartTimeDelayLatency = true;

    @Test
    public void shouldReturnCorrectMeasurements() throws WorkloadException, MetricsCollectionException {
        long initialTime = 0l;
        MetricsManager metricsManager = new MetricsManager(
                timeSource,
                TimeUnit.MILLISECONDS,
                initialTime,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_DELAY_DURATION_AS_MILLI,
                recordStartTimeDelayLatency
        );

        Operation<?> operation1 = DummyLdbcSnbInteractiveOperationInstances.read1();
        operation1.setScheduledStartTimeAsMilli(1l);
        OperationResultReport operationResultReport1 = OperationResultReportTestHelper.create(1, "result one", operation1);
        OperationResultReportTestHelper.setActualStartTime(operationResultReport1, 2l);
        OperationResultReportTestHelper.setRunDuration(operationResultReport1, temporalUtil.convert(1, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS));

        Operation<?> operation2 = DummyLdbcSnbInteractiveOperationInstances.read1();
        operation2.setScheduledStartTimeAsMilli(1l);
        OperationResultReport operationResultReport2 = OperationResultReportTestHelper.create(2, "result two", operation2);
        OperationResultReportTestHelper.setActualStartTime(operationResultReport2, 8l);
        OperationResultReportTestHelper.setRunDuration(operationResultReport2, temporalUtil.convert(3, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS));

        Operation<?> operation3 = DummyLdbcSnbInteractiveOperationInstances.read2();
        operation3.setScheduledStartTimeAsMilli(1l);
        OperationResultReport operationResultReport3 = OperationResultReportTestHelper.create(2, "result three", operation3);
        OperationResultReportTestHelper.setActualStartTime(operationResultReport3, 11l);
        OperationResultReportTestHelper.setRunDuration(operationResultReport3, temporalUtil.convert(5, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS));

        metricsManager.measure(operationResultReport1);
        metricsManager.measure(operationResultReport2);
        metricsManager.measure(operationResultReport3);

        assertThat(metricsManager.startTimeAsMilli(), equalTo(0l));
        assertThat(metricsManager.latestFinishTimeAsMilli(), equalTo(16l));
    }
}
