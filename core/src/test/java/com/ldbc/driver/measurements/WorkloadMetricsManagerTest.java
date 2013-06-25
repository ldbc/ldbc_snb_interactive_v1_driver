package com.ldbc.driver.measurements;

import org.junit.Test;

import com.ldbc.driver.OperationResult;
import com.ldbc.driver.measurements.exporters.MetricsExporter;
import com.ldbc.driver.measurements.exporters.OutputStreamMetricsExporter;
import com.ldbc.driver.measurements.formatters.MetricsFormatter;
import com.ldbc.driver.measurements.formatters.SimpleMetricsFormatter;
import com.ldbc.driver.util.Duration;
import com.ldbc.driver.util.Time;
import com.ldbc.driver.util.TimeUnit;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.matchers.JUnitMatchers.*;

public class WorkloadMetricsManagerTest
{

    @Test
    public void shouldReturnCorrectMeasurements() throws MetricsExporterException
    {
        WorkloadMetricsManager workloadMeasurements = new WorkloadMetricsManager( TimeUnit.NANO );

        OperationResult operationResult1 = new OperationResult( 1, "result one" );
        operationResult1.setOperationType( "type one" );
        operationResult1.setScheduledStartTime( Time.fromNano( 1 ) );
        operationResult1.setActualStartTime( Time.fromNano( 2 ) );
        operationResult1.setRunTime( Duration.fromNano( 1 ) );

        OperationResult operationResult2 = new OperationResult( 2, "result two" );
        operationResult2.setOperationType( "type one" );
        operationResult2.setScheduledStartTime( Time.fromNano( 1 ) );
        operationResult2.setActualStartTime( Time.fromNano( 8 ) );
        operationResult2.setRunTime( Duration.fromNano( 3 ) );

        OperationResult operationResult3 = new OperationResult( 2, "result three" );
        operationResult3.setOperationType( "type two" );
        operationResult3.setScheduledStartTime( Time.fromNano( 1 ) );
        operationResult3.setActualStartTime( Time.fromNano( 11 ) );
        operationResult3.setRunTime( Duration.fromNano( 5 ) );

        workloadMeasurements.measure( operationResult1 );
        workloadMeasurements.measure( operationResult2 );
        workloadMeasurements.measure( operationResult3 );

        int metricTypeCount = 0;
        for ( MetricGroup metricGroup : workloadMeasurements.getAllMeasurements() )
        {
            metricTypeCount++;
        }

        assertThat( metricTypeCount, is( 2 ) );

        MetricsFormatter formatter = new SimpleMetricsFormatter();
        MetricsExporter exporter = new OutputStreamMetricsExporter( System.out );
        exporter.export( formatter, workloadMeasurements.getAllMeasurements() );
    }
}
