package com.ldbc.driver.util;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.matchers.JUnitMatchers.*;

public class TimeTest
{
    @Test
    public void shouldEqualWhenSame()
    {
        // Given
        Time time1 = Time.fromNano( 1 );
        Time time2 = Time.fromNano( 1 );

        // When

        // Then
        assertThat( time1, equalTo( time2 ) );
    }

    @Test
    public void shouldNotEqualWhenDifferent()
    {
        // Given
        Time time1 = Time.fromNano( 1 );
        Time time2 = Time.fromNano( 2 );

        // When

        // Then
        assertThat( time1, not( equalTo( time2 ) ) );
    }

    @Test
    public void shouldBeSummable()
    {
        // Given
        Time time1 = Time.fromNano( 1 );
        Duration duration1 = Duration.fromNano( 1 );

        // When
        Time time2 = time1.plus( duration1 );

        // Then
        assertThat( time2.asNano(), is( 2l ) );
    }

    @Test
    public void shouldBeSubtractable()
    {
        // Given
        Time time1 = Time.fromNano( 1 );
        Duration duration1 = Duration.fromNano( 1 );

        // When
        Time time2 = time1.minus( duration1 );

        // Then
        assertThat( time2.asNano(), is( 0l ) );
    }

    @Test
    public void shouldConvertableBetweenUnits()
    {
        // Given
        Time time1 = Time.fromMilli( 1 );

        // When
        Time time2 = Time.fromNano( time1.asNano() );

        // Then
        assertThat( time1.asNano(), equalTo( time2.asNano() ) );
    }
}
