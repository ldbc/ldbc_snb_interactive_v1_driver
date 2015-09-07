package com.ldbc.driver.util;

import com.google.common.collect.Range;
import com.ldbc.driver.util.Bucket.NumberRangeBucket;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import static java.lang.String.format;

// THING_TYPE - Things - type of things being counted
//   ---> Bucket must be able to compare this
//   ---> Histogram must be able to get/put this
// COUNT - Count - type of Number used to count the Things
//   ---> Histogram must be able to get
//   ---> Histogram must be able to inc

public class Histogram<THING_TYPE, COUNT extends Number>
{
    private final Map<Bucket<THING_TYPE>,COUNT> valuedBuckets;
    private final NumberHelper<COUNT> number;
    private final COUNT defaultBucketValue;

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

    public static <THING_TYPES, COUNTS extends Number> boolean equalsWithinTolerance(
            Histogram<THING_TYPES,COUNTS> first, Histogram<THING_TYPES,COUNTS> second, Number tolerance )
    {
        if ( first == second )
        { return true; }
        if ( first == null )
        { return false; }
        if ( second == null )
        { return false; }
        if ( first.valuedBuckets == null )
        {
            if ( second.valuedBuckets != null )
            { return false; }
        }
        return doEqualsWithinTolerance( first, second, tolerance );
    }

    private static <THING_TYPES, COUNTS extends Number> boolean doEqualsWithinTolerance(
            Histogram<THING_TYPES,COUNTS> first, Histogram<THING_TYPES,COUNTS> second, Number tolerance )
    {
        if ( first.getBucketCount() != second.getBucketCount() )
        {
            return false;
        }
        for ( Bucket<THING_TYPES> firstBucket : first.valuedBuckets.keySet() )
        {
            COUNTS firstBucketValue = first.valuedBuckets.get( firstBucket );
            COUNTS secondBucketValue = second.valuedBuckets.get( firstBucket );

            if ( null == secondBucketValue )
            {
                return false;
            }

            if ( false == NumberHelper.withinTolerance( firstBucketValue, secondBucketValue, tolerance ) )
            {
                return false;
            }
        }
        return true;
    }

    public Histogram( COUNT defaultBucketValue )
    {
        this( new HashMap<Bucket<THING_TYPE>,COUNT>(), defaultBucketValue );
    }

    private Histogram( Map<Bucket<THING_TYPE>,COUNT> valuedBuckets, COUNT defaultBucketValue )
    {
        this.valuedBuckets = valuedBuckets;
        this.defaultBucketValue = defaultBucketValue;
        this.number = NumberHelper.createNumberHelper( defaultBucketValue.getClass() );
    }

    public void importValueSequence( Iterable<THING_TYPE> valueSequence )
    {
        importValueSequence( valueSequence.iterator() );
    }

    // TODO would be nice if there was a version of this method that added buckets when they don't yet exist
    public void importValueSequence( Iterator<THING_TYPE> valueSequence )
    {
        while ( valueSequence.hasNext() )
        {
            THING_TYPE value = valueSequence.next();
            Bucket<THING_TYPE> bucket = getExactlyOneBucketFor( value );
            incBucketValue( bucket, number.one() );
        }
    }

    public int getBucketCount()
    {
        return valuedBuckets.size();
    }

    public void addBuckets( Iterable<Bucket<THING_TYPE>> buckets )
    {
        addBuckets( buckets, defaultBucketValue );
    }

    public void addBuckets( Iterable<Bucket<THING_TYPE>> buckets, COUNT initialValue )
    {
        for ( Bucket<THING_TYPE> bucket : buckets )
        {
            addBucket( bucket, initialValue );
        }
    }

    public void addBucket( Bucket<THING_TYPE> bucket )
    {
        addBucket( bucket, defaultBucketValue );
    }

    public void addBucket( Bucket<THING_TYPE> bucket, COUNT initialValue )
    {
        valuedBuckets.put( bucket, initialValue );
    }

    public void setAllBucketValues( COUNT value )
    {
        setBucketValues( valuedBuckets.keySet(), value );
    }

    public void setBucketValues( Iterable<Bucket<THING_TYPE>> buckets, COUNT value )
    {
        for ( Bucket<THING_TYPE> bucket : buckets )
        {
            setBucketValue( bucket, value );
        }
    }

    public void setBucketValue( Bucket<THING_TYPE> bucket, COUNT value )
    {
        assertBucketExists( bucket );
        valuedBuckets.put( bucket, value );
    }

    // Returns new bucket value
    public COUNT incBucketValue( Bucket<THING_TYPE> bucket, COUNT amount )
    {
        assertBucketExists( bucket );
        return incBucketValueWithoutAssert( bucket, amount );
    }

    // Returns new bucket value
    public COUNT incOrCreateBucket( Bucket<THING_TYPE> bucket, COUNT amount )
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

    private COUNT incBucketValueWithoutAssert( Bucket<THING_TYPE> bucket, COUNT amount )
    {
        COUNT bucketValue = valuedBuckets.get( bucket );
        bucketValue = number.sum( bucketValue, amount );
        valuedBuckets.put( bucket, bucketValue );
        return bucketValue;
    }

    public COUNT getBucketValue( Bucket<THING_TYPE> bucket )
    {
        return valuedBuckets.get( bucket );
    }

    public Iterable<Entry<Bucket<THING_TYPE>,COUNT>> getAllBuckets()
    {
        return valuedBuckets.entrySet();
    }

    public COUNT getDefaultBucketValue()
    {
        return defaultBucketValue;
    }

    public COUNT sumOfAllBucketValues()
    {
        return number.sum( valuedBuckets.values() );
    }

    public Histogram<THING_TYPE,Double> toPercentageValues()
    {
        Map<Bucket<THING_TYPE>,Double> percentageValuedBuckets = new HashMap<Bucket<THING_TYPE>,Double>();
        Double sumOfAllBucketValues = number.sum( valuedBuckets.values() ).doubleValue();
        for ( Bucket<THING_TYPE> bucket : valuedBuckets.keySet() )
        {
            COUNT bucketValue = valuedBuckets.get( bucket );
            Double percentageBucketValue = bucketValue.doubleValue() / sumOfAllBucketValues;
            percentageValuedBuckets.put( bucket, percentageBucketValue );
        }
        return new Histogram<THING_TYPE,Double>( percentageValuedBuckets, 1d );
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

    private Bucket<THING_TYPE> getExactlyOneBucketFor( THING_TYPE value )
    {
        List<Tuple2<Bucket<THING_TYPE>,THING_TYPE>> bucketHits = new ArrayList<Tuple2<Bucket<THING_TYPE>,THING_TYPE>>();
        for ( Bucket<THING_TYPE> bucket : valuedBuckets.keySet() )
        {
            if ( bucket.contains( value ) )
            {
                bucketHits.add( Tuple.tuple2( bucket, value ) );
            }
        }
        if ( bucketHits.size() < 1 )
        {
            String errorMessage = format( "0 buckets found matching %s, expected 1. Buckets(%s) BucketHits(%s)",
                    value, valuedBuckets.keySet().toString(), bucketHits.toString() );
            throw new RuntimeException( errorMessage );
        }
        if ( bucketHits.size() > 1 )
        {
            String errorMessage = format(
                    "%s buckets found matching %s, expected 1. Buckets(%s) BucketHits(%s)", bucketHits.size(), value,
                    valuedBuckets.keySet().toString(), bucketHits.toString() );
            throw new RuntimeException( errorMessage );
        }
        return bucketHits.get( 0 )._1();
    }

    private void assertBucketExists( Bucket<THING_TYPE> bucket )
    {
        if ( false == valuedBuckets.containsKey( bucket ) )
        {
            throw new RuntimeException( format( "Bucket[%s] not found in Histogram", bucket ) );
        }
    }

    private Map<Bucket<THING_TYPE>,COUNT> copyAndSortByBucketSize( final Map<Bucket<THING_TYPE>,COUNT> map )
    {
        ValueComparator valueComparator = new ValueComparator( map );
        TreeMap<Bucket<THING_TYPE>,COUNT> sortedMap = new TreeMap<Bucket<THING_TYPE>,COUNT>( valueComparator );
        sortedMap.putAll( map );
        return sortedMap;
    }

    class ValueComparator implements Comparator<Bucket<THING_TYPE>>
    {
        Map<Bucket<THING_TYPE>,COUNT> base;

        public ValueComparator( Map<Bucket<THING_TYPE>,COUNT> base )
        {
            this.base = base;
        }

        @Override
        public int compare( Bucket<THING_TYPE> a, Bucket<THING_TYPE> b )
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
