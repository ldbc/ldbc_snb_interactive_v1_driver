package com.ldbc.driver.generator;

import com.google.common.collect.Iterators;
import com.ldbc.driver.util.Histogram;
import com.ldbc.driver.util.NumberHelper;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public abstract class GeneratorTest<GENERATE_TYPE, COUNT extends Number>
{
    private final long RANDOM_SEED = 42;
    private final int SAMPLE_SIZE = 1000000;
    private GeneratorFactory generatorFactory = null;

    public abstract Histogram<GENERATE_TYPE,COUNT> getExpectedDistribution();

    public abstract double getDistributionTolerance();

    public abstract Iterator<GENERATE_TYPE> getGeneratorImpl( GeneratorFactory generatorFactory );

    protected final int getSampleSize()
    {
        return SAMPLE_SIZE;
    }

    protected final GeneratorFactory getGeneratorFactory()
    {
        return generatorFactory;
    }

    @Before
    public final void initGeneratorFactory()
    {
        generatorFactory = new GeneratorFactory( new RandomDataGeneratorFactory( RANDOM_SEED ) );
    }

    @Test
    public final void determinismTest()
    {
        GeneratorFactory generatorFactoryA = new GeneratorFactory( new RandomDataGeneratorFactory( RANDOM_SEED ) );
        GeneratorFactory generatorFactoryB = new GeneratorFactory( new RandomDataGeneratorFactory( RANDOM_SEED ) );

        Iterator<GENERATE_TYPE> sequenceA = Iterators.limit( getGeneratorImpl( generatorFactoryA ), getSampleSize() );
        Iterator<GENERATE_TYPE> sequenceB = Iterators.limit( getGeneratorImpl( generatorFactoryB ), getSampleSize() );

        assertThat( Iterators.elementsEqual( sequenceA, sequenceB ), is( true ) );
    }

    @Test
    public final void distributionTest()
    {
        // Given
        GeneratorFactory generatorFactory = new GeneratorFactory( new RandomDataGeneratorFactory( RANDOM_SEED ) );
        Iterator<GENERATE_TYPE> generator = getGeneratorImpl( generatorFactory );
        Histogram<GENERATE_TYPE,COUNT> expectedDistribution = getExpectedDistribution();

        // When
        List<GENERATE_TYPE> generatedSequence = generateSequence( generator, getSampleSize() );
        // getExpectedDistribution() ensures same histogram dimensions/types
        Histogram<GENERATE_TYPE,COUNT> generatedDistribution = getExpectedDistribution();

        NumberHelper<COUNT> number =
                NumberHelper.createNumberHelper( generatedDistribution.getDefaultBucketValue().getClass() );
        generatedDistribution.setAllBucketValues( number.zero() );
        generatedDistribution.importValueSequence( generatedSequence );

        // Then
        Histogram<GENERATE_TYPE,Double> expectedDistributionAsPercentage = expectedDistribution.toPercentageValues();
        Histogram<GENERATE_TYPE,Double> generatedDistributionAsPercentage = generatedDistribution.toPercentageValues();

        String errMsg = format( "Distributions should be within tolerance[%s]\nExpected[%s]\nGenerated[%s]",
                getDistributionTolerance(), expectedDistributionAsPercentage, generatedDistributionAsPercentage );

        assertThat(
                errMsg,
                Histogram.equalsWithinTolerance( generatedDistributionAsPercentage, expectedDistributionAsPercentage,
                        getDistributionTolerance() ),
                is( true ) );
    }

    public final List<GENERATE_TYPE> generateSequence( Iterator<GENERATE_TYPE> generator, Integer size )
    {
        List<GENERATE_TYPE> generatedNumberSequence = new ArrayList<GENERATE_TYPE>();
        for ( int i = 0; i < size; i++ )
        {
            GENERATE_TYPE next = generator.next();
            generatedNumberSequence.add( next );
        }
        return generatedNumberSequence;
    }

}
