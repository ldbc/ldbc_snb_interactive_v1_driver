package org.ldbcouncil.driver.runtime.metrics;

import org.ldbcouncil.driver.Operation;
import org.ldbcouncil.driver.WorkloadException;
import org.ldbcouncil.driver.control.Log4jLoggingServiceFactory;
import org.ldbcouncil.driver.control.LoggingServiceFactory;
import org.ldbcouncil.driver.temporal.SystemTimeSource;
import org.ldbcouncil.driver.temporal.TimeSource;
import org.ldbcouncil.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration;
import org.ldbcouncil.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveOperationInstances;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class MetricsManagerTest
{
    private final TimeSource timeSource = new SystemTimeSource();
    private final LoggingServiceFactory loggingServiceFactory = new Log4jLoggingServiceFactory( false );

    @Test
    public void shouldReturnCorrectMeasurements() throws WorkloadException, MetricsCollectionException
    {
        MetricsManager metricsManager = new MetricsManager(
                timeSource,
                TimeUnit.MILLISECONDS,
                ThreadedQueuedMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO,
                LdbcSnbInteractiveWorkloadConfiguration.operationTypeToClassMapping(),
                loggingServiceFactory
        );

        Operation operation1 = DummyLdbcSnbInteractiveOperationInstances.read1();
        long operation1ActualStartTimeAsMilli = 2;
        long operation1RunDurationAsNano = TimeUnit.MILLISECONDS.toNanos( 1 );

        Operation operation2 = DummyLdbcSnbInteractiveOperationInstances.read1();
        long operation2ActualStartTimeAsMilli = 8;
        long operation2RunDurationAsNano = TimeUnit.MILLISECONDS.toNanos( 3 );

        Operation operation3 = DummyLdbcSnbInteractiveOperationInstances.read2();
        long operation3ActualStartTimeAsMilli = 11;
        long operation3RunDurationAsNano = TimeUnit.MILLISECONDS.toNanos( 5 );

        metricsManager.measure( operation1ActualStartTimeAsMilli, operation1RunDurationAsNano, operation1.type() );
        metricsManager.measure( operation2ActualStartTimeAsMilli, operation2RunDurationAsNano, operation2.type() );
        metricsManager.measure( operation3ActualStartTimeAsMilli, operation3RunDurationAsNano, operation3.type() );

        WorkloadResultsSnapshot snapshot = metricsManager.snapshot();
        assertThat( snapshot.startTimeAsMilli(), equalTo( 2l ) );
        assertThat( snapshot.latestFinishTimeAsMilli(), equalTo( 16l ) );
    }
}
