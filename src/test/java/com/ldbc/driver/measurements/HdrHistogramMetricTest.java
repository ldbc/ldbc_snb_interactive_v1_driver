package com.ldbc.driver.measurements;

import org.junit.Test;

import com.ldbc.driver.measurements.metric.ContinuousMetric;
import com.ldbc.driver.measurements.metric.Metric;
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
        Metric metric1 = new ContinuousMetric( 2l, 1 );
        metric1.addMeasurement( 31l );

        // Max without exception
        // 0000 0000 0000 1111 1111
        Metric metric2 = new ContinuousMetric( 2l, 2 );
        metric2.addMeasurement( 255l );

        // Max without exception
        // 0000 0000 0111 1111 1111
        Metric metric3 = new ContinuousMetric( 2l, 3 );
        metric3.addMeasurement( 2047l );

        // Max without exception
        // 0000 0111 1111 1111 1111
        Metric metric4 = new ContinuousMetric( 2l, 4 );
        metric4.addMeasurement( 32767l );

        // 0011 1111 1111 1111 1111
        Metric metric5 = new ContinuousMetric( 2l, 5 );
        metric5.addMeasurement( 262143l );
    }

    @Test
    public void shouldGiveExpectedMeasurements()
    {
        ContinuousMetric metric1 = new ContinuousMetric( 100000000l, 1 );
        metric1.addMeasurement( 31000000l );
        metric1.addMeasurement( 31000000l );
        metric1.addMeasurement( 31000000l );
        assertThat( withinPercentage( 31000000, metric1.min(), 0.10 ), is( true ) );
        assertThat( withinPercentage( 31000000, metric1.max(), 0.10 ), is( true ) );
        assertThat( withinPercentage( 31000000, metric1.mean(), 0.10 ), is( true ) );

        ContinuousMetric metric2 = new ContinuousMetric( 100000000l, 2 );
        metric2.addMeasurement( 31000000l );
        metric2.addMeasurement( 31000000l );
        metric2.addMeasurement( 31000000l );
        assertThat( withinPercentage( 31000000, metric2.min(), 0.01 ), is( true ) );
        assertThat( withinPercentage( 31000000, metric2.max(), 0.01 ), is( true ) );
        assertThat( withinPercentage( 31000000, metric2.mean(), 0.01 ), is( true ) );

        ContinuousMetric metric3 = new ContinuousMetric( 100000000l, 3 );
        metric3.addMeasurement( 31000000l );
        metric3.addMeasurement( 31000000l );
        metric3.addMeasurement( 31000000l );
        assertThat( withinPercentage( 31000000, metric3.min(), 0.001 ), is( true ) );
        assertThat( withinPercentage( 31000000, metric3.max(), 0.001 ), is( true ) );
        assertThat( withinPercentage( 31000000, metric3.mean(), 0.001 ), is( true ) );

        ContinuousMetric metric4 = new ContinuousMetric( 100000000l, 4 );
        metric4.addMeasurement( 31000000l );
        metric4.addMeasurement( 31000000l );
        metric4.addMeasurement( 31000000l );
        assertThat( withinPercentage( 31000000, metric4.min(), 0.0001 ), is( true ) );
        assertThat( withinPercentage( 31000000, metric4.max(), 0.0001 ), is( true ) );
        assertThat( withinPercentage( 31000000, metric4.mean(), 0.0001 ), is( true ) );

        ContinuousMetric metric5 = new ContinuousMetric( 100000000l, 5 );
        metric5.addMeasurement( 31000000l );
        metric5.addMeasurement( 31000000l );
        metric5.addMeasurement( 31000000l );
        assertThat( withinPercentage( 31000000, metric5.min(), 0.00001 ), is( true ) );
        assertThat( withinPercentage( 31000000, metric5.max(), 0.00001 ), is( true ) );
        assertThat( withinPercentage( 31000000, metric5.mean(), 0.00001 ), is( true ) );
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
        ContinuousMetric testMetric = new ContinuousMetric( "Test", "Some Unit",
                Duration.fromSeconds( 60 ).asNano(), 5 );

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

        assertThat( testMetric.count(), is( 10l ) );
        assertThat( testMetric.min(), is( 1l ) );
        assertThat( testMetric.max(), is( 10l ) );
        assertThat( testMetric.mean(), is( 5.5 ) );
        assertThat( testMetric.percentile( 20d ), is( 2l ) );
        assertThat( testMetric.percentile( 70d ), is( 7l ) );
        assertThat( testMetric.percentile( 90d ), is( 9l ) );

        // System.out.println( testMetric.toPrettyString() );
        // MetricsFormatter formatter = new SimpleMetricsFormatter();
        // MetricsExporter exporter = new OutputStreamMetricsExporter(
        // System.out );
        // exporter.export( formatter, testMetric );
    }
}
