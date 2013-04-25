package com.yahoo.ycsb.generator;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Range;

public class GeneratorTestUtils
{

    /**
     * Asserts
     */

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

    /**
     * Helpers
     */

    public static Map<Range<Double>, Double> fillBucketsUniformly( List<Range<Double>> bucketRanges, Double totalValue )
    {
        int bucketCount = bucketRanges.size();
        Map<Range<Double>, Double> buckets = new HashMap<Range<Double>, Double>();
        for ( Range<Double> bucket : bucketRanges )
        {
            buckets.put( bucket, totalValue / bucketCount );
        }
        return buckets;
    }

    public static <T extends Number> Double getSequenceMean( List<T> sequence )
    {
        int sequenceLength = sequence.size();
        double sum = 0d;
        for ( T number : sequence )
        {
            sum += number.doubleValue();
        }
        return sum / sequenceLength;
    }

    public static <T extends Number> List<T> makeSequence( Generator<T> generator, Integer size )
    {
        List<T> generatedNumberSequence = new ArrayList<T>();
        for ( int i = 0; i < size; i++ )
        {
            T next = generator.next();
            generatedNumberSequence.add( next );
        }
        return generatedNumberSequence;
    }
}
