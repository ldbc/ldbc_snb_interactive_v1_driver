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

public class HdrHistogramMetricTest
{

    @Test
    public void shouldNotThrowException()
    {
        // Max without exception
        // 0000 0000 0000 0001 1111
        Metric metric1 = new HdrHistogramMetric( 2l, 1 );
        metric1.addMeasurement( 31l );

        // Max without exception
        // 0000 0000 0000 1111 1111
        Metric metric2 = new HdrHistogramMetric( 2l, 2 );
        metric2.addMeasurement( 255l );

        // Max without exception
        // 0000 0000 0111 1111 1111
        Metric metric3 = new HdrHistogramMetric( 2l, 3 );
        metric3.addMeasurement( 2047l );

        // Max without exception
        // 0000 0111 1111 1111 1111
        Metric metric4 = new HdrHistogramMetric( 2l, 4 );
        metric4.addMeasurement( 32767l );

        // 0011 1111 1111 1111 1111
        Metric metric5 = new HdrHistogramMetric( 2l, 5 );
        metric5.addMeasurement( 262143l );
    }

    @Test
    public void shouldGiveExpectedMeasurements()
    {
        Metric metric1 = new HdrHistogramMetric( 100000000l, 1 );
        metric1.addMeasurement( 31000000l );
        metric1.addMeasurement( 31000000l );
        metric1.addMeasurement( 31000000l );
        assertThat( withinPercentage( 31000000, metric1.getMin(), 0.10 ), is( true ) );
        assertThat( withinPercentage( 31000000, metric1.getMax(), 0.10 ), is( true ) );
        assertThat( withinPercentage( 31000000, metric1.getMean(), 0.10 ), is( true ) );

        Metric metric2 = new HdrHistogramMetric( 100000000l, 2 );
        metric2.addMeasurement( 31000000l );
        metric2.addMeasurement( 31000000l );
        metric2.addMeasurement( 31000000l );
        assertThat( withinPercentage( 31000000, metric2.getMin(), 0.01 ), is( true ) );
        assertThat( withinPercentage( 31000000, metric2.getMax(), 0.01 ), is( true ) );
        assertThat( withinPercentage( 31000000, metric2.getMean(), 0.01 ), is( true ) );

        Metric metric3 = new HdrHistogramMetric( 100000000l, 3 );
        metric3.addMeasurement( 31000000l );
        metric3.addMeasurement( 31000000l );
        metric3.addMeasurement( 31000000l );
        assertThat( withinPercentage( 31000000, metric3.getMin(), 0.001 ), is( true ) );
        assertThat( withinPercentage( 31000000, metric3.getMax(), 0.001 ), is( true ) );
        assertThat( withinPercentage( 31000000, metric3.getMean(), 0.001 ), is( true ) );

        Metric metric4 = new HdrHistogramMetric( 100000000l, 4 );
        metric4.addMeasurement( 31000000l );
        metric4.addMeasurement( 31000000l );
        metric4.addMeasurement( 31000000l );
        assertThat( withinPercentage( 31000000, metric4.getMin(), 0.0001 ), is( true ) );
        assertThat( withinPercentage( 31000000, metric4.getMax(), 0.0001 ), is( true ) );
        assertThat( withinPercentage( 31000000, metric4.getMean(), 0.0001 ), is( true ) );

        Metric metric5 = new HdrHistogramMetric( 100000000l, 5 );
        metric5.addMeasurement( 31000000l );
        metric5.addMeasurement( 31000000l );
        metric5.addMeasurement( 31000000l );
        assertThat( withinPercentage( 31000000, metric5.getMin(), 0.00001 ), is( true ) );
        assertThat( withinPercentage( 31000000, metric5.getMax(), 0.00001 ), is( true ) );
        assertThat( withinPercentage( 31000000, metric5.getMean(), 0.00001 ), is( true ) );
    }

    private boolean withinPercentage( double expectedValue, double actualValue, double percentage )
    {
        double tolerance = expectedValue * percentage;
        double difference = Math.abs( expectedValue - actualValue );
        // System.out.println( String.format( "Exp[%s] Act[%s] Tol[%s] Dif[%s]",
        // expectedValue, actualValue, tolerance,
        // difference ) );
        return difference <= tolerance;
    }

    @Test
    public void shouldReturnCorrectMeasurements() throws MetricsExporterException
    {
        Metric testMetric = new HdrHistogramMetric( "Test", "Some Unit", Duration.fromSeconds( 60 ).asNano(), 5 );

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
        assertThat( testMetric.getCountAt( 0 ), is( 0l ) );
        assertThat( testMetric.getCountAt( 1 ), is( 1l ) );

        // System.out.println( testMetric.toPrettyString() );
        // MetricsFormatter formatter = new SimpleMetricsFormatter();
        // MetricsExporter exporter = new OutputStreamMetricsExporter(
        // System.out );
        // exporter.export( formatter, testMetric );
    }
}
