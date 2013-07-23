package com.ldbc.driver.measurements;

import org.junit.Test;

import com.ldbc.driver.measurements.metric.HdrHistogramMetric;
import com.ldbc.driver.measurements.metric.HdrHistogramMetricFactory;
import com.ldbc.driver.measurements.metric.Metric;
import com.ldbc.driver.util.temporal.Duration;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.matchers.JUnitMatchers.*;

public class MetricGroupTest
{

    @Test
    public void shouldReturnCorrectMeasurements() throws MetricsExporterException
    {
        MetricGroup<HdrHistogramMetric> metricGroup = new MetricGroup<HdrHistogramMetric>( "Test",
                new HdrHistogramMetricFactory( "Some Unit", Duration.fromSeconds( 60 ).asNano(), 5 ) );

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

        HdrHistogramMetric operation1Metrics = (HdrHistogramMetric) metricGroup.getMetric( "Operation1" );
        HdrHistogramMetric operation2Metrics = (HdrHistogramMetric) metricGroup.getMetric( "Operation2" );
        assertThat( metricGroup.getName(), is( "Test" ) );
        assertThat( size, is( 2 ) );
        assertThat( operation1Metrics.getCount(), is( 5l ) );
        assertThat( operation1Metrics.getMin(), is( 1l ) );
        assertThat( operation1Metrics.getMax(), is( 8l ) );
        assertThat( operation2Metrics.getCount(), is( 4l ) );
        assertThat( operation2Metrics.getMean(), is( 6d ) );

        // MetricsFormatter formatter = new SimpleMetricsFormatter();
        // MetricsExporter exporter = new OutputStreamMetricsExporter(
        // System.out );
        // exporter.export( formatter, metricGroup );
    }
}
