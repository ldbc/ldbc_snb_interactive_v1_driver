package com.ldbc.driver.generator;

import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import com.ldbc.driver.util.RandomDataGeneratorFactory;

import static org.junit.Assert.assertEquals;

public class MinMaxGeneratorWrapperTest
{
    private final long RANDOM_SEED = 42;
    private GeneratorFactory generatorFactory = null;

    @Before
    public final void initGeneratorFactory()
    {
        generatorFactory = new GeneratorFactory( new RandomDataGeneratorFactory( RANDOM_SEED ) );
    }

    @Test
    public void createMinMaxWrapperTest()
    {
        Iterator<Integer> generator = generatorFactory.constantGenerator( 5 );
        Iterator<Integer> minMaxGenerator = new MinMaxGenerator<Integer>( generator, 1, 10 );
        assertEquals( false, generator instanceof MinMaxGenerator );
        assertEquals( true, minMaxGenerator instanceof MinMaxGenerator );
        assertEquals( 5, (int) minMaxGenerator.next() );
    }

    @Test
    public void minMaxTest()
    {
        Iterator<Integer> generator = generatorFactory.incrementingGenerator( 5, 1 );
        MinMaxGenerator<Integer> minMaxGenerator = new MinMaxGenerator<Integer>( generator, 10, 5 );
        assertEquals( 10, (int) minMaxGenerator.getMin() );
        assertEquals( 5, (int) minMaxGenerator.getMax() );
        assertEquals( 5, (int) minMaxGenerator.next() );
        assertEquals( 6, (int) minMaxGenerator.next() );
        assertEquals( 5, (int) minMaxGenerator.getMin() );
        assertEquals( 6, (int) minMaxGenerator.getMax() );
    }
}
