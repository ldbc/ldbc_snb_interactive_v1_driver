package com.ldbc.driver.measurements;

import java.util.ArrayList;
import java.util.List;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramData;
import org.HdrHistogram.HistogramIterationValue;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.matchers.JUnitMatchers.*;

public class MeasurementsTest
{

    @Test
    public void shouldHdrInstance()
    {
        Histogram histogram = new Histogram( 10, 3 );

        histogram.recordValue( 1 );
        histogram.recordValue( 1 );
        histogram.recordValue( 1 );
        histogram.recordValue( 2 );
        histogram.recordValue( 2 );
        histogram.recordValue( 6 );
        histogram.recordValue( 9 );
        histogram.recordValue( 9 );

        System.out.println( "------------" );
        HistogramData histogramData = histogram.getHistogramData();
        System.out.println( "histogramData.getMinValue() " + histogramData.getMinValue() );
        System.out.println( "histogramData.getMaxValue() " + histogramData.getMaxValue() );
        System.out.println( "histogramData.getMean() " + histogramData.getMean() );
        System.out.println( "histogramData.getCountAtValue( 3 ) " + histogramData.getCountAtValue( 3 ) );
        System.out.println( "histogramData.getCountAtValue( 9 ) " + histogramData.getCountAtValue( 9 ) );
        System.out.println( "histogramData.getCountBetweenValues( 3, 5 ) " + histogramData.getCountBetweenValues( 3, 5 ) );
        System.out.println( "histogramData.getCountBetweenValues( 3, 6 ) " + histogramData.getCountBetweenValues( 3, 6 ) );
        System.out.println( "histogramData.getCountBetweenValues( 3, 7 ) " + histogramData.getCountBetweenValues( 3, 7 ) );
        System.out.println( "histogramData.getPercentileAtOrBelowValue( 3 ) "
                            + histogramData.getPercentileAtOrBelowValue( 3 ) );
        System.out.println( "histogramData.getPercentileAtOrBelowValue( 6 ) "
                            + histogramData.getPercentileAtOrBelowValue( 6 ) );
        System.out.println( "histogramData.getPercentileAtOrBelowValue( 8 ) "
                            + histogramData.getPercentileAtOrBelowValue( 8 ) );
        System.out.println( "histogramData.getPercentileAtOrBelowValue( 9 ) "
                            + histogramData.getPercentileAtOrBelowValue( 9 ) );
        System.out.println( "histogramData.getStdDeviation() " + histogramData.getStdDeviation() );
        System.out.println( "histogramData.getTotalCount() " + histogramData.getTotalCount() );
        System.out.println( "histogramData.getValueAtPercentile( 50 ) " + histogramData.getValueAtPercentile( 50 ) );
        System.out.println( "histogramData.getValueAtPercentile( 75 ) " + histogramData.getValueAtPercentile( 75 ) );
        System.out.println( "histogramData.getValueAtPercentile( 90 ) " + histogramData.getValueAtPercentile( 90 ) );
        System.out.println( "histogramData.getValueAtPercentile( 95 ) " + histogramData.getValueAtPercentile( 95 ) );
        System.out.println( "histogramData.getValueAtPercentile( 99 ) " + histogramData.getValueAtPercentile( 99 ) );
        System.out.println( "histogramData.getValueAtPercentile( 99.9 ) " + histogramData.getValueAtPercentile( 99.9 ) );
        System.out.println( "------------" );
        System.out.println( "histogramData.recordedValues()..." );
        for ( HistogramIterationValue histogramIterationValue : histogramData.recordedValues() )
        {
            System.out.println( "histogramIterationValue.getPercentile() " + histogramIterationValue.getPercentile() );
            System.out.println( "--" );
        }
        System.out.println( "------------" );
        System.out.println( "histogram.highestEquivalentValue( 5 ) " + histogram.highestEquivalentValue( 5 ) );
        System.out.println( "histogram.lowestEquivalentValue( 5 ) " + histogram.lowestEquivalentValue( 5 ) );
        System.out.println( "histogram.medianEquivalentValue( 5 ) " + histogram.medianEquivalentValue( 5 ) );
        System.out.println( "histogram.nextNonEquivalentValue( 5 ) " + histogram.nextNonEquivalentValue( 5 ) );
    }
}
