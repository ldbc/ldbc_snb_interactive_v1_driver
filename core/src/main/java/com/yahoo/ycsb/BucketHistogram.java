package com.yahoo.ycsb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Range;
import com.yahoo.ycsb.Bucket.NumberRangeBucket;
import com.yahoo.ycsb.generator.GeneratorException;

// TODO remove?
// T - Things - type of things being counted
//   ---> Bucket must be able to compare this
//   ---> Histogram must be able to get/put this
// C - Count - type of Number used to count the Things
//   ---> Histogram must be able to get
//   ---> Histogram must be able to inc

public class BucketHistogram<T, C extends Number>
{
    private final Map<Bucket<T>, C> valuedBuckets;
    private final NumberHelper<C> number;
    private final C defaultBucketValue;

    public static <N extends Number> List<Bucket<N>> makeEqualBucketRanges( Double min, Double max,
            Integer bucketCount, Class<N> bucketType )
    {
        List<Bucket<N>> buckets = new ArrayList<Bucket<N>>();
        Double interval = max - min;
        Double bucketInterval = interval / bucketCount;
        Double lowerBound = min;
        Double upperBound = lowerBound + bucketInterval;
        for ( int i = 0; i < bucketCount - 1; i++ )
        {
            // [a..b) <--> {x | a <= x < b}
            Bucket<N> bucket = new NumberRangeBucket<N>( Range.closedOpen( lowerBound, upperBound ) );
            buckets.add( bucket );
            lowerBound = upperBound;
            upperBound = lowerBound + bucketInterval;
        }
        // [a..b] <--> {x | a <= x <= b}
        Bucket<N> bucket = new NumberRangeBucket<N>( Range.closed( lowerBound, max ) );
        buckets.add( bucket );
        return buckets;
    }

    public BucketHistogram( C defaultBucketValue )
    {
        this( new HashMap<Bucket<T>, C>(), defaultBucketValue );
    }

    private BucketHistogram( Map<Bucket<T>, C> valuedBuckets, C defaultBucketValue )
    {
        this.valuedBuckets = valuedBuckets;
        this.defaultBucketValue = defaultBucketValue;
        this.number = NumberHelper.createNumberHelper( defaultBucketValue.getClass() );
    }

    public void importValueSequence( Iterable<T> numberSequence )
    {
        for ( T value : numberSequence )
        {
            Bucket<T> bucket = getBucketFor( value );
            incBucketValue( bucket, number.one() );
        }
    }

    public int getBucketCount()
    {
        return valuedBuckets.size();
    }

    public void addBuckets( Iterable<Bucket<T>> buckets )
    {
        addBuckets( buckets, defaultBucketValue );
    }

    public void addBuckets( Iterable<Bucket<T>> buckets, C initialValue )
    {
        for ( Bucket<T> bucket : buckets )
        {
            addBucket( bucket, initialValue );
        }
    }

    public void addBucket( Bucket<T> bucket )
    {
        addBucket( bucket, defaultBucketValue );
    }

    public void addBucket( Bucket<T> bucket, C initialValue )
    {
        valuedBuckets.put( bucket, initialValue );
    }

    public void setAllBucketValues( C value )
    {
        setBucketValues( valuedBuckets.keySet(), value );
    }

    public void setBucketValues( Iterable<Bucket<T>> buckets, C value )
    {
        for ( Bucket<T> bucket : buckets )
        {
            setBucketValue( bucket, value );
        }
    }

    public void setBucketValue( Bucket<T> bucket, C value )
    {
        assertBucketExists( bucket );
        valuedBuckets.put( bucket, value );
    }

    // Returns new bucket value
    public C incBucketValue( Bucket<T> bucket, C amount )
    {
        assertBucketExists( bucket );
        C bucketValue = valuedBuckets.get( bucket );
        bucketValue = number.sum( bucketValue, amount );
        valuedBuckets.put( bucket, bucketValue );
        return bucketValue;
    }

    public C getBucketValue( Bucket<T> bucket )
    {
        return valuedBuckets.get( bucket );
    }

    public BucketHistogram<T, Double> toPercentageValues()
    {
        Map<Bucket<T>, Double> percentageValuedBuckets = new HashMap<Bucket<T>, Double>();
        Double sumOfAllBucketValues = number.sum( valuedBuckets.values() ).doubleValue();
        for ( Bucket<T> bucket : valuedBuckets.keySet() )
        {
            C bucketValue = valuedBuckets.get( bucket );
            Double percentageBucketValue = bucketValue.doubleValue() / sumOfAllBucketValues;
            percentageValuedBuckets.put( bucket, percentageBucketValue );
        }
        return new BucketHistogram<T, Double>( percentageValuedBuckets, 1d );
    }

    @Override
    public String toString()
    {
        return "Histogram [valuedBuckets=" + valuedBuckets + ", defaultBucketValue=" + defaultBucketValue + "]";
    }

    private Bucket<T> getBucketFor( T value )
    {
        List<Pair<Bucket<T>, T>> bucketHits = new ArrayList<Pair<Bucket<T>, T>>();
        for ( Bucket<T> bucket : valuedBuckets.keySet() )
        {
            if ( bucket.contains( value ) )
            {
                bucketHits.add( new Pair<Bucket<T>, T>( bucket, value ) );
            }
        }
        if ( bucketHits.size() < 1 )
        {
            String errorMessage = String.format( "0 buckets found matching %s, expected 1. Buckets(%s) BucketHits(%s)",
                    value, valuedBuckets.keySet().toString(), bucketHits.toString() );
            throw new RuntimeException( errorMessage );
        }
        if ( bucketHits.size() > 1 )
        {
            String errorMessage = String.format(
                    "%s buckets found matching %s, expected 1. Buckets(%s) BucketHits(%s)", bucketHits.size(), value,
                    valuedBuckets.keySet().toString(), bucketHits.toString() );
            throw new RuntimeException( errorMessage );
        }
        return bucketHits.get( 0 )._1();
    }

    private void assertBucketExists( Bucket<T> bucket )
    {
        if ( false == valuedBuckets.containsKey( bucket ) )
        {
            // TODO other Exception type should be thrown?
            throw new GeneratorException( String.format( "Bucket[%s] not found in Histogram", bucket ) );
        }
    }

    public boolean equalsWithinTolerance( BucketHistogram<T, C> other, Number tolerance )
    {
        if ( this == other ) return true;
        if ( other == null ) return false;
        if ( valuedBuckets == null )
        {
            if ( other.valuedBuckets != null ) return false;
        }
        return doEqualsWithinTolerance( other, tolerance );
    }

    private boolean doEqualsWithinTolerance( BucketHistogram<T, C> other, Number tolerance )
    {
        if ( other.getBucketCount() != this.getBucketCount() )
        {
            // String errMsg = String.format(
            // "Histograms should contain the same amount of buckets [%s,%s]",
            // this.getBucketCount(), other.getBucketCount() );
            // System.out.println( errMsg );
            return false;
        }
        for ( Bucket<T> thisBucket : this.valuedBuckets.keySet() )
        {
            C thisBucketValue = this.valuedBuckets.get( thisBucket );
            C otherBucketValue = other.valuedBuckets.get( thisBucket );

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
