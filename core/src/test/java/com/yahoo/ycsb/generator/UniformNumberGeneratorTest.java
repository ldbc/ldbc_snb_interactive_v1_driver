package com.yahoo.ycsb.generator;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Range;
import com.yahoo.ycsb.generator.GeneratorFactory;

public class UniformNumberGeneratorTest
{
    GeneratorFactory generatorFactory = null;

    @Before
    public void initGeneratorFactory()
    {
        generatorFactory = new AbstractGeneratorFactory( new RandomDataGenerator() ).newGeneratorFactory();
    }

    @Test
    public void proportionsConstructorTest()
    {
        // Given
        long min = 0;
        long max = 100;
        int bucketCount = 5;
        int sampleSize = 100000;

        List<Range<Double>> bucketRanges;
        bucketRanges = GeneratorTestUtils.makeEqualBucketRanges( (double) min, (double) max, bucketCount );
        // bucketRanges = new ArrayList<Range<Double>>();
        // bucketRanges.add( Range.closedOpen( 0d, 20d ) );
        // bucketRanges.add( Range.closedOpen( 20d, 40d ) );
        // bucketRanges.add( Range.closedOpen( 40d, 60d ) );
        // bucketRanges.add( Range.closedOpen( 60d, 80d ) );
        // bucketRanges.add( Range.closed( 80d, 100d ) );

        Map<Range<Double>, Double> expectedBuckets = new HashMap<Range<Double>, Double>();
        expectedBuckets.put( bucketRanges.get( 0 ), 0.2 );
        expectedBuckets.put( bucketRanges.get( 1 ), 0.2 );
        expectedBuckets.put( bucketRanges.get( 2 ), 0.2 );
        expectedBuckets.put( bucketRanges.get( 3 ), 0.2 );
        expectedBuckets.put( bucketRanges.get( 4 ), 0.2 );

        double tolerance = 0.01;

        // When
        UniformNumberGenerator<Long> generator = generatorFactory.newUniformNumberGenerator( min, max );
        List<Long> generatedSequence = GeneratorTestUtils.makeSequence( generator, sampleSize );
        Map<Range<Double>, Double> generatedBuckets = GeneratorTestUtils.sequenceToBuckets( generatedSequence,
                bucketRanges );

        // Then
        GeneratorTestUtils.assertDistributionCorrect( expectedBuckets, generatedBuckets, tolerance );
    }

    @Test
    public void nextLastTest()
    {
        // Given
        double min = 0.0;
        double max = 100.0;
        int timesToTestLast = 1000;

        // When
        UniformNumberGenerator<Double> generator = generatorFactory.newUniformNumberGenerator( min, max );

        // Then
        GeneratorTestUtils.assertLastEqualsLastNext( generator, timesToTestLast );
    }
}
