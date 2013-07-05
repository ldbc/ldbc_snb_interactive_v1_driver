package com.ldbc.driver.measurements;

import org.junit.Test;

import com.ldbc.driver.measurements.exporters.MetricsExporter;
import com.ldbc.driver.measurements.exporters.OutputStreamMetricsExporter;
import com.ldbc.driver.measurements.formatters.MetricsFormatter;
import com.ldbc.driver.measurements.formatters.SimpleMetricsFormatter;
import com.ldbc.driver.util.temporal.Duration;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.matchers.JUnitMatchers.*;

public class MetricGroupTest
{

    @Test
    public void shouldReturnCorrectMeasurements() throws MetricsExporterException
    {
        MetricGroup metricGroup = new MetricGroup( "Test", new HdrHistogramMetricFactory(
                Duration.fromSeconds( 60 ).asNano(), 5 ) );

        Metric operation1TestMetric = metricGroup.getOrCreateMetric( "Operation1" );
        operation1TestMetric = metricGroup.getOrCreateMetric( "Operation1" );
        Metric operation2TestMetric = metricGroup.getOrCreateMetric( "Operation2" );

        operation1TestMetric.addMeasurement( 1 );
        operation1TestMetric.addMeasurement( 4 );
        operation1TestMetric.addMeasurement( 5 );
        operation1TestMetric.addMeasurement( 6 );
        operation1TestMetric.addMeasurement( 8 );

        operation2TestMetric.addMeasurement( 2 );
        operation2TestMetric.addMeasurement( 3 );
        operation2TestMetric.addMeasurement( 7 );
        operation2TestMetric.addMeasurement( 12 );

        int size = 0;
        for ( Metric metric : metricGroup.getMetrics() )
        {
            size++;
        }

        assertThat( metricGroup.getName(), is( "Test" ) );
        assertThat( size, is( 2 ) );
        assertThat( metricGroup.getOrCreateMetric( "Operation1" ).getCount(), is( 5l ) );
        assertThat( metricGroup.getOrCreateMetric( "Operation1" ).getMin(), is( 1l ) );
        assertThat( metricGroup.getOrCreateMetric( "Operation1" ).getMax(), is( 8l ) );
        assertThat( metricGroup.getOrCreateMetric( "Operation2" ).getCount(), is( 4l ) );
        assertThat( metricGroup.getOrCreateMetric( "Operation2" ).getMean(), is( 6d ) );

        // MetricsFormatter formatter = new SimpleMetricsFormatter();
        // MetricsExporter exporter = new OutputStreamMetricsExporter(
        // System.out );
        // exporter.export( formatter, metricGroup );
    }
}
