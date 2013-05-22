package com.ldbc.generator;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorBuilder;
import com.ldbc.driver.generator.GeneratorBuilderFactory;
import com.ldbc.driver.util.Histogram;
import com.ldbc.driver.util.NumberHelper;

import static org.junit.Assert.assertEquals;

public abstract class GeneratorTest<T, C extends Number>
{
    private final int SAMPLE_SIZE = 1000000;
    private GeneratorBuilder generatorBuilder = null;

    public abstract Histogram<T, C> getExpectedDistribution();

    public abstract double getDistributionTolerance();

    public abstract Generator<T> getGeneratorImpl();

    protected final int getSampleSize()
    {
        return SAMPLE_SIZE;
    }

    protected final GeneratorBuilder getGeneratorBuilder()
    {
        return generatorBuilder;
    }

    @Before
    public final void initGeneratorFactory()
    {
        generatorBuilder = new GeneratorBuilderFactory( new RandomDataGenerator() ).newGeneratorBuilder();
    }

    @Test
    public final void distributionTest()
    {
        // Given
        Generator<T> generator = getGeneratorImpl();
        Histogram<T, C> expectedDistribution = getExpectedDistribution();

        // When
        List<T> generatedSequence = generateSequence( generator, getSampleSize() );
        Histogram<T, C> generatedDistribution = getExpectedDistribution();
        NumberHelper<C> number = NumberHelper.createNumberHelper( generatedDistribution.getDefaultBucketValue().getClass() );
        generatedDistribution.setAllBucketValues( number.zero() );
        generatedDistribution.importValueSequence( generatedSequence );

        // Then
        Histogram<T, Double> expectedDistributionAsPercentage = expectedDistribution.toPercentageValues();
        Histogram<T, Double> generatedDistributionAsPercentage = generatedDistribution.toPercentageValues();

        String errMsg = String.format( "Distributions should be within tolerance[%s]\nExpected[%s]\nGenerated[%s]",
                getDistributionTolerance(), expectedDistributionAsPercentage, generatedDistributionAsPercentage );
        assertEquals( errMsg, true, generatedDistributionAsPercentage.equalsWithinTolerance(
                expectedDistributionAsPercentage, getDistributionTolerance() ) );
    }

    public final List<T> generateSequence( Generator<T> generator, Integer size )
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
