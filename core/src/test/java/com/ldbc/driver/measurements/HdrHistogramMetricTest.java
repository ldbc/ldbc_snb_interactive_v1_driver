package com.ldbc.driver.measurements;

import org.junit.Test;

import com.ldbc.driver.measurements.exporters.MetricsExporter;
import com.ldbc.driver.measurements.exporters.OutputStreamMetricsExporter;
import com.ldbc.driver.measurements.formatters.MetricsFormatter;
import com.ldbc.driver.measurements.formatters.SimpleMetricsFormatter;
import com.ldbc.driver.util.Duration;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.matchers.JUnitMatchers.*;

public class HdrHistogramMetricTest
{

    @Test
    public void shouldReturnCorrectMeasurements() throws MetricsExporterException
    {
        Metric testMetric = new HdrHistogramMetric( "Test", Duration.fromSeconds( 60 ) );

        testMetric.addMeasurement( 1 );
        testMetric.addMeasurement( 2 );
        testMetric.addMeasurement( 3 );
        testMetric.addMeasurement( 4 );
        testMetric.addMeasurement( 5 );
        testMetric.addMeasurement( 6 );
        testMetric.addMeasurement( 7 );
        testMetric.addMeasurement( 8 );
        testMetric.addMeasurement( 9 );
        testMetric.addMeasurement( 10 );

        assertThat( testMetric.getCount(), is( 10l ) );
        assertThat( testMetric.getMin(), is( 1l ) );
        assertThat( testMetric.getMax(), is( 10l ) );
        assertThat( testMetric.getMean(), is( 5.5 ) );
        assertThat( testMetric.getPercentile( 20d ), is( 2l ) );
        assertThat( testMetric.getPercentile( 70d ), is( 7l ) );
        assertThat( testMetric.getPercentile( 90d ), is( 9l ) );

        System.out.println( testMetric.toPrettyString() );
        MetricsFormatter formatter = new SimpleMetricsFormatter();
        MetricsExporter exporter = new OutputStreamMetricsExporter( System.out );
        exporter.export( formatter, testMetric );
    }
}
