package com.yahoo.ycsb;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Range;

public class HistogramTest
{
    @Test
    public void makeEqualBucketRangesTest()
    {
        // Given
        double min = 0;
        double max = 10;
        int bucketCount = 5;

        // When
        List<Range<Double>> buckets = Histogram.makeEqualBucketRanges( min, max, bucketCount );

        // Then
        Double expectedBucketRange = ( max - min ) / bucketCount;
        assertEquals( bucketCount, buckets.size() );
        for ( Range<Double> bucket : buckets )
        {
            Double bucketRange = bucket.upperEndpoint() - bucket.lowerEndpoint();
            assertEquals( expectedBucketRange, bucketRange );
        }
    }

    @Test
    public void createEmpty()
    {
        // Given

        // When
        Histogram<Integer> h = new Histogram<Integer>( 0 );

        // Then
        assertEquals( 0, h.getBucketCount() );
    }

    @Test
    public void importSequenceTest()
    {
        // Given
        Histogram<Integer> histogram = new Histogram<Integer>( 0 );
        histogram.addBucket( Range.closedOpen( 0d, 2d ) ); // 3
        histogram.addBucket( Range.closedOpen( 2d, 4d ) ); // 2
        histogram.addBucket( Range.closedOpen( 4d, 6d ) ); // 0
        histogram.addBucket( Range.closed( 6d, 8d ) ); // 1

        Integer[] sequenceArray = new Integer[] { 0, 1, 1, 2, 3, 8 };
        List<Integer> sequence = Arrays.asList( sequenceArray );

        assertEquals( 0, (int) histogram.getBucketValue( Range.closedOpen( 0d, 2d ) ) );
        assertEquals( 0, (int) histogram.getBucketValue( Range.closedOpen( 2d, 4d ) ) );
        assertEquals( 0, (int) histogram.getBucketValue( Range.closedOpen( 4d, 6d ) ) );
        assertEquals( 0, (int) histogram.getBucketValue( Range.closed( 6d, 8d ) ) );

        // When
        histogram.importValueSequence( sequence );

        // Then
        assertEquals( 3, (int) histogram.getBucketValue( Range.closedOpen( 0d, 2d ) ) );
        assertEquals( 2, (int) histogram.getBucketValue( Range.closedOpen( 2d, 4d ) ) );
        assertEquals( 0, (int) histogram.getBucketValue( Range.closedOpen( 4d, 6d ) ) );
        assertEquals( 1, (int) histogram.getBucketValue( Range.closed( 6d, 8d ) ) );

        assertEquals( 4, histogram.getBucketCount() );
    }

    @Test
    public void addBucketsTest()
    {
        // Given
        Histogram<Integer> histogram = new Histogram<Integer>( 2 );

        Range<Double>[] buckets = new Range[] { Range.closedOpen( 0d, 2d ), Range.closedOpen( 2d, 4d ),
                Range.closedOpen( 4d, 6d ), Range.closed( 6d, 8d ) };

        assertEquals( 0, histogram.getBucketCount() );
        // When
        histogram.addBuckets( Arrays.asList( buckets ) );

        // Then
        assertEquals( 4, histogram.getBucketCount() );
        for ( Range<Double> bucket : buckets )
        {
            assertEquals( 2, (int) histogram.getBucketValue( bucket ) );
        }
    }

    @Test
    public void addThenGetBucketTest()
    {
        // Given
        Histogram<Integer> histogram = new Histogram<Integer>( 5 );

        assertEquals( 0, histogram.getBucketCount() );

        // When
        Range<Double> bucket = Range.closedOpen( 0d, 2d );
        histogram.addBucket( bucket );

        // Then
        assertEquals( 1, histogram.getBucketCount() );
        assertEquals( 5, (int) histogram.getBucketValue( bucket ) );
    }

    @Test
    public void setAllBucketValuesTest()
    {
        // Given
        Histogram<Integer> histogram = new Histogram<Integer>( 2 );

        Range<Double>[] buckets = new Range[] { Range.closedOpen( 0d, 2d ), Range.closedOpen( 2d, 4d ),
                Range.closedOpen( 4d, 6d ), Range.closed( 6d, 8d ) };

        histogram.addBuckets( Arrays.asList( buckets ) );

        for ( Range<Double> bucket : buckets )
        {
            assertEquals( 2, (int) histogram.getBucketValue( bucket ) );
        }

        // When
        histogram.setAllBucketValues( 9 );

        // Then
        assertEquals( 4, histogram.getBucketCount() );
        for ( Range<Double> bucket : buckets )
        {
            assertEquals( 9, (int) histogram.getBucketValue( bucket ) );
        }
    }

    @Test
    public void setBucketValuesTest()
    {
        // Given
        Histogram<Integer> histogram = new Histogram<Integer>( 2 );

        Range<Double>[] buckets = new Range[] { Range.closedOpen( 0d, 2d ), Range.closedOpen( 2d, 4d ),
                Range.closedOpen( 4d, 6d ), Range.closed( 6d, 8d ) };

        histogram.addBuckets( Arrays.asList( buckets ), 2 );
        for ( Range<Double> bucket : buckets )
        {
            assertEquals( 2, (int) histogram.getBucketValue( bucket ) );
        }

        // When
        histogram.setBucketValues( Arrays.asList( buckets ), 9 );

        // Then
        assertEquals( 4, histogram.getBucketCount() );
        for ( Range<Double> bucket : buckets )
        {
            assertEquals( 9, (int) histogram.getBucketValue( bucket ) );
        }
    }

    @Test
    public void incBucketValueTest()
    {
        // Given
        Histogram<Integer> histogram = new Histogram<Integer>( 5 );

        assertEquals( 0, histogram.getBucketCount() );

        Range<Double> bucket = Range.closedOpen( 0d, 2d );
        histogram.addBucket( bucket );

        assertEquals( 1, histogram.getBucketCount() );
        assertEquals( 5, (int) histogram.getBucketValue( bucket ) );

        // When
        histogram.incBucketValue( bucket, 1 );

        // Then
        assertEquals( 1, histogram.getBucketCount() );
        assertEquals( 6, (int) histogram.getBucketValue( bucket ) );
    }

    @Test
    public void toPercentageValuesTest()
    {
        // Given
        Range<Double> bucket1 = Range.closedOpen( 0d, 2d );
        Range<Double> bucket2 = Range.closedOpen( 2d, 4d );
        Range<Double> bucket3 = Range.closed( 4d, 6d );
        Histogram<Integer> histogramAbsolute = new Histogram<Integer>( 0 );
        histogramAbsolute.addBucket( bucket1, 1 );
        histogramAbsolute.addBucket( bucket2, 1 );
        histogramAbsolute.addBucket( bucket3, 2 );

        assertEquals( 1, (int) histogramAbsolute.getBucketValue( bucket1 ) );
        assertEquals( 1, (int) histogramAbsolute.getBucketValue( bucket2 ) );
        assertEquals( 2, (int) histogramAbsolute.getBucketValue( bucket3 ) );

        // When
        Histogram<Double> histogramPercentage = histogramAbsolute.toPercentageValues();

        // Then
        assertEquals( 1, (int) histogramAbsolute.getBucketValue( bucket1 ) );
        assertEquals( 1, (int) histogramAbsolute.getBucketValue( bucket2 ) );
        assertEquals( 2, (int) histogramAbsolute.getBucketValue( bucket3 ) );

        assertEquals( (Double) 0.25, histogramPercentage.getBucketValue( bucket1 ) );
        assertEquals( (Double) 0.25, histogramPercentage.getBucketValue( bucket2 ) );
        assertEquals( (Double) 0.5, histogramPercentage.getBucketValue( bucket3 ) );
    }

    @Test
    public void equalsWithinToleranceTest()
    {
        // Given
        Range<Double> bucket1 = Range.closedOpen( 0d, 2d );
        Range<Double> bucket2 = Range.closedOpen( 2d, 4d );
        Histogram<Double> histogram1 = new Histogram<Double>( 0d );
        histogram1.addBucket( bucket1, 0.5 );
        histogram1.addBucket( bucket2, 0.2 );
        Histogram<Double> histogram2 = new Histogram<Double>( 0d );
        histogram2.addBucket( bucket1, 0.6 );
        histogram2.addBucket( bucket2, 0.2 );

        // When
        assertEquals( true, histogram1.equalsWithinTolerance( histogram2, 0.1 ) );
        assertEquals( false, histogram1.equalsWithinTolerance( histogram2, 0.05 ) );
        assertEquals( true, histogram2.equalsWithinTolerance( histogram1, 0.1 ) );
        assertEquals( false, histogram2.equalsWithinTolerance( histogram1, 0.05 ) );
    }

}
