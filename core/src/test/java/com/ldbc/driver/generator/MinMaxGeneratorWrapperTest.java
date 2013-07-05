package com.ldbc.driver.generator;

import org.junit.Before;
import org.junit.Test;

import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorBuilder;
import com.ldbc.driver.generator.wrapper.MinMaxGeneratorWrapper;
import com.ldbc.driver.util.RandomDataGeneratorFactory;

import static org.junit.Assert.assertEquals;

public class MinMaxGeneratorWrapperTest
{
    private final long RANDOM_SEED = 42;
    private GeneratorBuilder generatorBuilder = null;

    @Before
    public final void initGeneratorFactory()
    {
        generatorBuilder = new GeneratorBuilder( new RandomDataGeneratorFactory( RANDOM_SEED ) );
    }

    @Test
    public void createMinMaxWrapperTest()
    {
        Generator<Integer> generator = generatorBuilder.constantGenerator( 5 ).build();
        Generator<Integer> minMaxGenerator = new MinMaxGeneratorWrapper<Integer>( generator, 1, 10 );
        assertEquals( false, generator instanceof MinMaxGeneratorWrapper );
        assertEquals( true, minMaxGenerator instanceof MinMaxGeneratorWrapper );
        assertEquals( 5, (int) minMaxGenerator.next() );
    }

    @Test
    public void minMaxTest()
    {
        Generator<Integer> generator = generatorBuilder.incrementingGenerator( 5, 1 ).build();
        MinMaxGeneratorWrapper<Integer> minMaxGenerator = new MinMaxGeneratorWrapper<Integer>( generator, 10, 5 );
        assertEquals( 10, (int) minMaxGenerator.getMin() );
        assertEquals( 5, (int) minMaxGenerator.getMax() );
        assertEquals( 5, (int) minMaxGenerator.next() );
        assertEquals( 6, (int) minMaxGenerator.next() );
        assertEquals( 5, (int) minMaxGenerator.getMin() );
        assertEquals( 6, (int) minMaxGenerator.getMax() );
    }
}
