package com.ldbc.driver.util;

import org.junit.Test;

import com.ldbc.driver.util.temporal.Duration;
import com.ldbc.driver.util.temporal.TemporalException;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class DurationTest
{
    @Test
    public void shouldThrowExceptionIfOverflowDuringUnitConversion()
    {
        // Given
        /*
         * 9223372036854775807      Long.MAX_VALUE
         * 
         * 9223372036854775807      ns
         * 9223372036854775         us
         * 9223372036854            ms
         * 9223372036               s
         * 153722867                m (should NOT overflow)
         * 153722868                m (SHOULD overflow)
         */
        boolean exceptionThrownOnValidValue = false;
        long validMinutes = 153722867;
        boolean exceptionThrownOnInvalidValue = false;
        long invalidMinutes = 153722868;

        // When
        try
        {
            Duration.fromMinutes( validMinutes );
        }
        catch ( TemporalException e )
        {
            exceptionThrownOnValidValue = true;
        }
        try
        {
            Duration.fromMinutes( invalidMinutes );
        }
        catch ( TemporalException e )
        {
            exceptionThrownOnInvalidValue = true;
        }

        // Then
        assertThat( exceptionThrownOnValidValue, is( false ) );
        assertThat( exceptionThrownOnInvalidValue, is( true ) );
    }

    @Test
    public void shouldEqualWhenSame()
    {
        // Given
        Duration duration1 = Duration.fromNano( 1 );
        Duration duration2 = Duration.fromNano( 1 );

        // When

        // Then
        assertThat( duration1, equalTo( duration2 ) );
    }

    @Test
    public void shouldNotEqualWhenDifferent()
    {
        // Given
        Duration duration1 = Duration.fromNano( 1 );
        Duration duration2 = Duration.fromNano( 2 );

        // When

        // Then
        assertThat( duration1, not( equalTo( duration2 ) ) );
    }

    @Test
    public void shouldBeSummable()
    {
        // Given
        Duration duration1 = Duration.fromNano( 1 );
        Duration duration2 = Duration.fromNano( 1 );

        // When
        Duration duration3 = duration1.plus( duration2 );

        // Then
        assertThat( duration3.asNano(), is( 2l ) );
    }

    @Test
    public void shouldBeSubtractable()
    {
        // Given
        Duration duration1 = Duration.fromNano( 1 );
        Duration duration2 = Duration.fromNano( 1 );

        // When
        Duration duration3 = duration1.minus( duration2 );

        // Then
        assertThat( duration3.asNano(), is( 0l ) );
    }

    @Test
    public void shouldConvertableBetweenUnits()
    {
        // Given
        Duration duration1 = Duration.fromMilli( 1 );

        // When
        Duration duration2 = Duration.fromNano( duration1.asNano() );

        // Then
        assertThat( duration1.asNano(), equalTo( duration2.asNano() ) );
    }
}
