package com.ldbc.driver.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.google.common.collect.Range;
import com.ldbc.driver.util.Bucket.DiscreteBucket;
import com.ldbc.driver.util.Bucket.NumberRangeBucket;

// T - Things - type of things being counted
//   ---> Bucket must be able to compare this
//   ---> Histogram must be able to get/put this
// C - Count - type of Number used to count the Things
//   ---> Histogram must be able to get
//   ---> Histogram must be able to inc

public class Histogram<T, C extends Number>
{
    private final Map<Bucket<T>, C> valuedBuckets;
    private final NumberHelper<C> number;
    private final C defaultBucketValue;

    public static <N extends Number> List<Bucket<N>> makeBucketsOfEqualRange( Double min, Double max,
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

    public Histogram( C defaultBucketValue )
    {
        this( new HashMap<Bucket<T>, C>(), defaultBucketValue );
    }

    private Histogram( Map<Bucket<T>, C> valuedBuckets, C defaultBucketValue )
    {
        this.valuedBuckets = valuedBuckets;
        this.defaultBucketValue = defaultBucketValue;
        this.number = NumberHelper.createNumberHelper( defaultBucketValue.getClass() );
    }

    public void importValueSequence( Iterable<T> numberSequence )
    {
        importValueSequence( numberSequence.iterator() );
    }

    public void importValueSequence( Iterator<T> numberSequence )
    {
        while ( numberSequence.hasNext() )
        {
            T value = numberSequence.next();
            Bucket<T> bucket = getExactlyOneBucketFor( value );
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
        return incBucketValueWithoutAssert( bucket, amount );
    }

    // Returns new bucket value
    public C incOrCreateBucket( Bucket<T> bucket, C amount )
    {
        if ( valuedBuckets.containsKey( bucket ) )
        {
            return incBucketValueWithoutAssert( bucket, amount );
        }
        else
        {
            addBucket( bucket, amount );
            return amount;
        }
    }

    private C incBucketValueWithoutAssert( Bucket<T> bucket, C amount )
    {
        C bucketValue = valuedBuckets.get( bucket );
        bucketValue = number.sum( bucketValue, amount );
        valuedBuckets.put( bucket, bucketValue );
        return bucketValue;
    }

    public C getBucketValue( Bucket<T> bucket )
    {
        return valuedBuckets.get( bucket );
    }

    public Iterable<Entry<Bucket<T>, C>> getAllBuckets()
    {
        return valuedBuckets.entrySet();
    }

    public C getDefaultBucketValue()
    {
        return defaultBucketValue;
    }

    public C sumOfAllBucketValues()
    {
        return number.sum( valuedBuckets.values() );
    }

    public Histogram<T, Double> toPercentageValues()
    {
        Map<Bucket<T>, Double> percentageValuedBuckets = new HashMap<Bucket<T>, Double>();
        Double sumOfAllBucketValues = number.sum( valuedBuckets.values() ).doubleValue();
        for ( Bucket<T> bucket : valuedBuckets.keySet() )
        {
            C bucketValue = valuedBuckets.get( bucket );
            Double percentageBucketValue = bucketValue.doubleValue() / sumOfAllBucketValues;
            percentageValuedBuckets.put( bucket, percentageBucketValue );
        }
        return new Histogram<T, Double>( percentageValuedBuckets, 1d );
    }

    @Override
    public String toString()
    {
        return "Histogram [sumOfAllBucketValues=" + sumOfAllBucketValues() + ", valuedBuckets=" + valuedBuckets
               + ", defaultBucketValue=" + defaultBucketValue + "]";
    }

    public String toPrettyString()
    {
        return toPrettyString( "" );
    }

    public String toPrettyString( String prefix )
    {
        StringBuilder prettyStringBuilder = new StringBuilder();
        prettyStringBuilder.append( prefix + "Histogram\n" );
        prettyStringBuilder.append( prefix + "\tdefaultBucketValue=" + defaultBucketValue + "\n" );
        prettyStringBuilder.append( prefix + "\tbucketCount=" + getBucketCount() + "\n" );
        prettyStringBuilder.append( prefix + "\tsumOfAllBucketValues=" + sumOfAllBucketValues() + "\n" );
        prettyStringBuilder.append( MapUtils.prettyPrint( copyAndSortByBucketSize( valuedBuckets ), prefix + "\t" ) );
        return prettyStringBuilder.toString();
    }

    private Bucket<T> getExactlyOneBucketFor( T value )
    {
        List<Pair<Bucket<T>, T>> bucketHits = new ArrayList<Pair<Bucket<T>, T>>();
        for ( Bucket<T> bucket : valuedBuckets.keySet() )
        {
            if ( bucket.contains( value ) )
            {
                bucketHits.add( Pair.create( bucket, value ) );
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
            throw new RuntimeException( String.format( "Bucket[%s] not found in Histogram", bucket ) );
        }
    }

    public boolean equalsWithinTolerance( Histogram<T, C> other, Number tolerance )
    {
        if ( this == other ) return true;
        if ( other == null ) return false;
        if ( valuedBuckets == null )
        {
            if ( other.valuedBuckets != null ) return false;
        }
        return doEqualsWithinTolerance( other, tolerance );
    }

    private boolean doEqualsWithinTolerance( Histogram<T, C> other, Number tolerance )
    {
        if ( other.getBucketCount() != this.getBucketCount() )
        {
            return false;
        }
        for ( Bucket<T> thisBucket : this.valuedBuckets.keySet() )
        {
            C thisBucketValue = this.valuedBuckets.get( thisBucket );
            C otherBucketValue = other.valuedBuckets.get( thisBucket );

            if ( null == otherBucketValue )
            {
                return false;
            }

            if ( false == NumberHelper.withinTolerance( thisBucketValue, otherBucketValue, tolerance ) )
            {
                return false;
            }
        }
        return true;
    }

    private Map<Bucket<T>, C> copyAndSortByBucketSize( final Map<Bucket<T>, C> map )
    {
        ValueComparator valueComparator = new ValueComparator( map );
        TreeMap<Bucket<T>, C> sortedMap = new TreeMap<Bucket<T>, C>( valueComparator );
        sortedMap.putAll( map );
        return sortedMap;
    }

    class ValueComparator implements Comparator<Bucket<T>>
    {
        Map<Bucket<T>, C> base;

        public ValueComparator( Map<Bucket<T>, C> base )
        {
            this.base = base;
        }

        @Override
        public int compare( Bucket<T> a, Bucket<T> b )
        {
            if ( number.gt( base.get( a ), base.get( b ) ) )
            {
                return -1;
            }
            else
            {
                return 1;
            }
        }
    }

}
