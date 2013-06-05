package com.ldbc.driver.util;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.util.NumberHelper;

public class NumberHelperTest
{
    @Test
    public void createNumberHelperTest()
    {
        // Given

        // When

        // Then
        assertEquals( Double.class, NumberHelper.createNumberHelper( new Double( 1 ).getClass() ).zero().getClass() );
        assertEquals( Integer.class, NumberHelper.createNumberHelper( new Integer( 1 ).getClass() ).zero().getClass() );
        assertEquals( Long.class, NumberHelper.createNumberHelper( new Long( 1 ).getClass() ).zero().getClass() );
    }

    @Test
    public void createNumberHelperUnsupportedTypeTest()
    {
        // Given
        boolean exceptionAlwaysThrown = true;

        // When
        try
        {
            NumberHelper.createNumberHelper( new Byte( (byte) 1 ).getClass() );
            exceptionAlwaysThrown = false;
        }
        catch ( GeneratorException e )
        {
        }
        try
        {
            NumberHelper.createNumberHelper( new Float( 1 ).getClass() );
            exceptionAlwaysThrown = false;
        }
        catch ( GeneratorException e )
        {
        }
        try
        {
            NumberHelper.createNumberHelper( new Short( (short) 1 ).getClass() );
            exceptionAlwaysThrown = false;
        }
        catch ( GeneratorException e )
        {
        }
        try
        {
            NumberHelper.createNumberHelper( new BigInteger( "1" ).getClass() );
            exceptionAlwaysThrown = false;
        }
        catch ( GeneratorException e )
        {
        }
        try
        {
            NumberHelper.createNumberHelper( new AtomicInteger( 1 ).getClass() );
            exceptionAlwaysThrown = false;
        }
        catch ( GeneratorException e )
        {
        }
        try
        {
            NumberHelper.createNumberHelper( new AtomicLong( 1 ).getClass() );
            exceptionAlwaysThrown = false;
        }
        catch ( GeneratorException e )
        {
        }
        try
        {
            NumberHelper.createNumberHelper( new BigDecimal( 1 ).getClass() );
            exceptionAlwaysThrown = false;
        }
        catch ( GeneratorException e )
        {
        }

        // Then
        assertEquals( true, exceptionAlwaysThrown );
    }

    @Test
    public void functionsTest()
    {
        // Given
        NumberHelper<Integer> number = NumberHelper.createNumberHelper( Integer.class );

        // When

        // Then
        assertEquals( 0, (int) number.zero() );
        assertEquals( 1, (int) number.one() );
        assertEquals( 1, (int) number.inc( number.zero() ) );
        assertEquals( 2, (int) number.sum( number.one(), number.one() ) );
        assertEquals( 2, (int) number.sum( Arrays.asList( new Integer[] { 1, 1 } ) ) );
        assertEquals( true, NumberHelper.withinTolerance( 2, 4, 2.0 ) );
        assertEquals( false, NumberHelper.withinTolerance( 2, 4, 1.0 ) );
        assertEquals( true, NumberHelper.withinTolerance( 2l, 4l, 2.0 ) );
        assertEquals( false, NumberHelper.withinTolerance( 2l, 4l, 1.0 ) );
        assertEquals( true, NumberHelper.withinTolerance( 2d, 4d, 2.0 ) );
        assertEquals( false, NumberHelper.withinTolerance( 2d, 4d, 1.0 ) );
    }
}
