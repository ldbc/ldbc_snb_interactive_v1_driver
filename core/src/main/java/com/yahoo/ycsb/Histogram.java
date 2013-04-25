package com.yahoo.ycsb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Range;
import com.yahoo.ycsb.generator.GeneratorException;

public class Histogram<N extends Number>
{
    private final Map<Range<Double>, N> valuedBuckets;
    private final NumberHelper<N> number;
    private final N defaultBucketValue;

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
        Range<Double> bucket = Range.closed( lowerBound, max );
        buckets.add( bucket );
        return buckets;
    }

    public Histogram( N defaultBucketValue )
    {
        this( new HashMap<Range<Double>, N>(), defaultBucketValue );
    }

    private Histogram( Map<Range<Double>, N> valuedBuckets, N defaultBucketValue )
    {
        this.valuedBuckets = valuedBuckets;
        this.defaultBucketValue = defaultBucketValue;
        this.number = NumberHelper.createNumberHelper( defaultBucketValue.getClass() );
    }

    public void importValueSequence( Iterable<N> numberSequence )
    {
        for ( N value : numberSequence )
        {
            Range<Double> bucket = getBucketFor( value );
            incBucketValue( bucket, number.one() );
        }
    }

    public int getBucketCount()
    {
        return valuedBuckets.size();
    }

    public void addBuckets( Iterable<Range<Double>> buckets )
    {
        addBuckets( buckets, defaultBucketValue );
    }

    public void addBuckets( Iterable<Range<Double>> buckets, N initialValue )
    {
        for ( Range<Double> bucket : buckets )
        {
            addBucket( bucket, initialValue );
        }
    }

    public void addBucket( Range<Double> bucket )
    {
        addBucket( bucket, defaultBucketValue );
    }

    public void addBucket( Range<Double> bucket, N initialValue )
    {
        valuedBuckets.put( bucket, initialValue );
    }

    public void setAllBucketValues( N value )
    {
        setBucketValues( valuedBuckets.keySet(), value );
    }

    public void setBucketValues( Iterable<Range<Double>> buckets, N value )
    {
        for ( Range<Double> bucket : buckets )
        {
            setBucketValue( bucket, value );
        }
    }

    public void setBucketValue( Range<Double> bucket, N value )
    {
        assertBucketExists( bucket );
        valuedBuckets.put( bucket, value );
    }

    // Returns new bucket value
    public N incBucketValue( Range<Double> bucket, N amount )
    {
        assertBucketExists( bucket );
        N bucketValue = valuedBuckets.get( bucket );
        bucketValue = number.sum( bucketValue, amount );
        valuedBuckets.put( bucket, bucketValue );
        return bucketValue;
    }

    public N getBucketValue( Range<Double> bucket )
    {
        return valuedBuckets.get( bucket );
    }

    public Histogram<Double> toPercentageValues()
    {
        Map<Range<Double>, Double> percentageValuedBuckets = new HashMap<Range<Double>, Double>();
        Double sumOfAllBucketValues = number.sum( valuedBuckets.values() ).doubleValue();
        for ( Range<Double> bucket : valuedBuckets.keySet() )
        {
            N bucketValue = valuedBuckets.get( bucket );
            Double percentageBucketValue = bucketValue.doubleValue() / sumOfAllBucketValues;
            percentageValuedBuckets.put( bucket, percentageBucketValue );
        }
        return new Histogram<Double>( percentageValuedBuckets, 1d );
    }

    @Override
    public String toString()
    {
        return "Histogram [valuedBuckets=" + valuedBuckets + ", defaultBucketValue=" + defaultBucketValue + "]";
    }

    private Range<Double> getBucketFor( N value )
    {
        List<Pair<Range<Double>, N>> bucketHits = new ArrayList<Pair<Range<Double>, N>>();
        for ( Range<Double> bucket : valuedBuckets.keySet() )
        {
            if ( bucket.contains( value.doubleValue() ) )
            {
                bucketHits.add( new Pair<Range<Double>, N>( bucket, value ) );
            }
        }
        if ( bucketHits.size() < 1 )
        {
            String errorMessage = String.format( "0 buckets found matching %s, expected 1. Buckets(%s) BucketHits(%s)",
                    value.doubleValue(), valuedBuckets.keySet().toString(), bucketHits.toString() );
            throw new RuntimeException( errorMessage );
        }
        if ( bucketHits.size() > 1 )
        {
            String errorMessage = String.format(
                    "%s buckets found matching %s, expected 1. Buckets(%s) BucketHits(%s)", bucketHits.size(),
                    value.doubleValue(), valuedBuckets.keySet().toString(), bucketHits.toString() );
            throw new RuntimeException( errorMessage );
        }
        return bucketHits.get( 0 )._1();
    }

    private void assertBucketExists( Range<Double> bucket )
    {
        if ( false == valuedBuckets.containsKey( bucket ) )
        {
            // TODO other Exception type should be thrown?
            throw new GeneratorException( String.format( "Bucket[%s] not found in Histogram", bucket ) );
        }
    }

    public boolean equalsWithinTolerance( Histogram<N> other, Double tolerance )
    {
        if ( this == other ) return true;
        if ( other == null ) return false;
        if ( valuedBuckets == null )
        {
            if ( other.valuedBuckets != null ) return false;
        }
        return doEqualsWithinTolerance( other, tolerance );
    }

    private boolean doEqualsWithinTolerance( Histogram<N> other, Double tolerance )
    {
        if ( other.getBucketCount() != this.getBucketCount() )
        {
            // String errMsg = String.format(
            // "Histograms should contain the same amount of buckets [%s,%s]",
            // this.getBucketCount(), other.getBucketCount() );
            // System.out.println( errMsg );
            return false;
        }
        for ( Range<Double> thisBucket : this.valuedBuckets.keySet() )
        {
            N thisBucketValue = this.valuedBuckets.get( thisBucket );
            N otherBucketValue = other.valuedBuckets.get( thisBucket );

            if ( null == otherBucketValue )
            {
                // String errMsg = String.format(
                // "Histograms should contain the same set of buckets [%s]",
                // thisBucket );
                // System.out.println( errMsg );
                return false;
            }

            if ( false == NumberHelper.withinTolerance( thisBucketValue, otherBucketValue, tolerance ) )
            {
                // String errMsg = String.format(
                // "Bucket values [%s,%s] should be within tolerance [%s] of each other",
                // thisBucketValue, otherBucketValue, tolerance );
                // System.out.println( errMsg );
                return false;
            }
        }
        return true;
    }
}
