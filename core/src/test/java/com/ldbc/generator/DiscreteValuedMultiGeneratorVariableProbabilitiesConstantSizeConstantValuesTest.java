package com.ldbc.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import com.ldbc.generator.Generator;
import com.ldbc.util.Histogram;
import com.ldbc.util.Triple;
import com.ldbc.util.Bucket.DiscreteBucket;

public class DiscreteValuedMultiGeneratorVariableProbabilitiesConstantSizeConstantValuesTest extends
        GeneratorTest<Map<String, Long>, Integer>
{

    @Override
    public Histogram<Map<String, Long>, Integer> getExpectedDistribution()
    {
        // 12 & 21
        Map<String, Long> m12 = new HashMap<String, Long>();
        m12.put( "1", 1l );
        m12.put( "2", 2l );

        // 13 & 31
        Map<String, Long> m13 = new HashMap<String, Long>();
        m13.put( "1", 1l );
        m13.put( "3", 3l );

        // 23 & 32
        Map<String, Long> m23 = new HashMap<String, Long>();
        m23.put( "2", 2l );
        m23.put( "3", 3l );

        Histogram<Map<String, Long>, Integer> expectedDistribution = new Histogram<Map<String, Long>, Integer>( 0 );

        expectedDistribution.addBucket( DiscreteBucket.create( m12 ), 25 );
        expectedDistribution.addBucket( DiscreteBucket.create( m13 ), 10 );
        expectedDistribution.addBucket( DiscreteBucket.create( m23 ), 25 );
        return expectedDistribution;
    }

    @Override
    public double getDistributionTolerance()
    {
        return 0.01;
    }

    @Override
    public Generator<Map<String, Long>> getGeneratorImpl()
    {
        Triple<Double, String, Generator<Long>> t1 = Triple.create( 1.0, "1",
                (Generator<Long>) getGeneratorBuilder().constantGenerator( 1l ).build() );
        Triple<Double, String, Generator<Long>> t2 = Triple.create( 2.0, "2",
                (Generator<Long>) getGeneratorBuilder().constantGenerator( 2l ).build() );
        Triple<Double, String, Generator<Long>> t3 = Triple.create( 1.0, "3",
                (Generator<Long>) getGeneratorBuilder().constantGenerator( 3l ).build() );

        ArrayList<Triple<Double, String, Generator<Long>>> items = new ArrayList<Triple<Double, String, Generator<Long>>>();
        items.add( t1 );
        items.add( t2 );
        items.add( t3 );
        Integer amountToRetrieve = 2;
        DiscreteValuedMultiGenerator<String, Long> generator = getGeneratorBuilder().discreteValuedMultiGenerator(
                items, amountToRetrieve ).build();
        return generator;
    }
}
