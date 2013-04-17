package com.yahoo.ycsb.generator;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Range;

public class GeneratorTestUtils
{

    public static <T> void assertLastEqualsLastNext( Generator<T> generator, int timesToTestLast )
    {
        // Given

        // When
        T last = generator.next();
        assertEquals( "last() should equal previous next()", last, generator.last() );
        boolean lastEqualsPreviousNext = true;
        for ( int i = 0; i < timesToTestLast; i++ )
        {
            last = generator.next();
            lastEqualsPreviousNext = lastEqualsPreviousNext && ( last.equals( generator.last() ) );
        }

        // Then
        assertEquals( "last() should always equal the previous next()", true, lastEqualsPreviousNext );
    }

    public static <T extends Number> void assertDistributionCorrect( Map<Range<Double>, Double> expectedBuckets,
            Map<Range<Double>, Double> generatedBuckets, Double tolerance )
    {
        assertEquals( "Bucket sets should be of same size", expectedBuckets.size(), generatedBuckets.size() );
        for ( Range<Double> expectedBucket : expectedBuckets.keySet() )
        {
            Double expectedBucketValue = expectedBuckets.get( expectedBucket );
            Double generatedBucketValue = generatedBuckets.get( expectedBucket );
            assertEquals( "Generated bucket set should contain all expected buckets", false,
                    null == generatedBucketValue );
            String assertMessage = String.format(
                    "Generated bucket value[%s] should be within tolerance[%s] of expected bucket value[%s]",
                    generatedBucketValue, tolerance, expectedBucketValue );
            assertWithinTolerance( assertMessage, expectedBucketValue, generatedBucketValue, tolerance );
        }
    }

    public static void assertWithinTolerance( String message, Double expectedBucketValue, Double generatedBucketValue,
            Double tolerance )
    {
        Double difference = Math.abs( ( (double) expectedBucketValue - (double) generatedBucketValue ) );
        assertEquals( message, true, difference <= tolerance );

    }

    public static <T extends Number> Map<Range<Double>, Double> sequenceToBuckets( List<T> sequence,
            List<Range<Double>> bucketList )
    {
        Map<Range<Double>, Double> buckets = new HashMap<Range<Double>, Double>();
        // Buckets as total value/count
        for ( T number : sequence )
        {
            Range<Double> bucket = getBucket( number, bucketList );
            buckets = incrementBucketEntry( buckets, bucket );
        }
        // Buckets as percentages of total value/count
        for ( Range<Double> bucket : buckets.keySet() )
        {
            Double bucketValue = buckets.get( bucket );
            Double percentageBucketValue = bucketValue / sequence.size();
            buckets.put( bucket, percentageBucketValue );
        }
        return buckets;
    }

    public static Map<Range<Double>, Double> incrementBucketEntry( Map<Range<Double>, Double> buckets,
            Range<Double> bucket )
    {
        Double bucketCount = buckets.get( bucket );
        bucketCount = ( null == bucketCount ) ? 1 : bucketCount + 1;
        buckets.put( bucket, bucketCount );
        return buckets;
    }

    public static <T extends Number> Range<Double> getBucket( T number, List<Range<Double>> bucketList )
    {
        List<Range<Double>> bucketHits = new ArrayList<Range<Double>>();
        for ( Range<Double> bucket : bucketList )
        {
            if ( bucket.contains( number.doubleValue() ) )
            {
                bucketHits.add( bucket );
            }
        }
        if ( bucketHits.size() < 1 )
        {
            String errorMessage = String.format( "0 buckets found matching %s, expected 1. Buckets(%s) BucketHits(%s)",
                    number.doubleValue(), bucketList.toString(), bucketHits.toString() );
            throw new RuntimeException( errorMessage );
        }
        if ( bucketHits.size() > 1 )
        {
            String errorMessage = String.format(
                    "%s buckets found matching %s, expected 1. Buckets(%s) BucketHits(%s)", bucketHits.size(),
                    number.doubleValue(), bucketList.toString(), bucketHits.toString() );
            throw new RuntimeException( errorMessage );
        }
        return bucketHits.get( 0 );
    }

    public static List<Range<Double>> makeEqualBucketRanges( Double min, Double max, Integer bucketCount )
    {
        List<Range<Double>> buckets = new ArrayList<Range<Double>>();
        Double interval = max - min;
        Double bucketInterval = interval / bucketCount;
        Double lowerBound = min;
        Double upperBound = lowerBound + bucketInterval;
        for ( int i = 0; i < bucketCount - 1; i++ )
        {
            // [a..b) <--> {x | a <= x < b}
            Range<Double> bucket = Range.closedOpen( lowerBound, upperBound );
            buckets.add( bucket );
            lowerBound = upperBound;
            upperBound = lowerBound + bucketInterval;
        }
        // [a..b] <--> {x | a <= x <= b}
        Range<Double> bucket = Range.closed( lowerBound, upperBound );
        buckets.add( bucket );
        Collections.sort( buckets, new BucketComparator() );
        return buckets;
    }

    public static <T extends Number> List<T> makeSequence( Generator<T> generator, Integer size )
    {
        List<T> generatedNumberSequence = new ArrayList<T>();
        for ( int i = 0; i < size; i++ )
        {
            generatedNumberSequence.add( generator.next() );
        }
        return generatedNumberSequence;
    }

    private static class BucketComparator implements Comparator<Range<Double>>
    {
        public int compare( Range<Double> bucket1, Range<Double> bucket2 )
        {
            return bucket1.lowerEndpoint() < bucket2.lowerEndpoint() ? -1 : 1;
        }
    }
}
