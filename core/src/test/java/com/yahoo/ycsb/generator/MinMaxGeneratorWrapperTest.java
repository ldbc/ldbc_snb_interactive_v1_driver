package com.yahoo.ycsb.generator;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class MinMaxGeneratorWrapperTest
{
    private GeneratorBuilder generatorBuilder = null;

    @Before
    public final void initGeneratorFactory()
    {
        generatorBuilder = new GeneratorBuilderFactory( new RandomDataGenerator() ).newGeneratorBuilder();
    }

    @Test
    public void createMinMaxWrapperTest()
    {
        Generator<Integer> generator = generatorBuilder.newConstantIntegerGenerator( 5 ).build();
        Generator<Integer> minMaxGenerator = new MinMaxGeneratorWrapper<Integer>( generator, 1, 10 );
        assertEquals( false, generator instanceof MinMaxGeneratorWrapper );
        assertEquals( true, minMaxGenerator instanceof MinMaxGeneratorWrapper );
        assertEquals( 5, (int) minMaxGenerator.next() );
    }

    @Test
    public void minMaxTest()
    {
        Generator<Integer> generator = generatorBuilder.newCounterGenerator( 5, 1 ).build();
        MinMaxGeneratorWrapper<Integer> minMaxGenerator = new MinMaxGeneratorWrapper<Integer>( generator, 10, 5 );
        assertEquals( 10, (int) minMaxGenerator.getMin() );
        assertEquals( 5, (int) minMaxGenerator.getMax() );
        assertEquals( 5, (int) minMaxGenerator.next() );
        assertEquals( 6, (int) minMaxGenerator.next() );
        assertEquals( 5, (int) minMaxGenerator.getMin() );
        assertEquals( 6, (int) minMaxGenerator.getMax() );
    }
}
