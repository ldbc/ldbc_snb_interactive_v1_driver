package com.ldbc.driver.util;

import org.junit.Test;

import com.ldbc.driver.util.temporal.Duration;
import com.ldbc.driver.util.temporal.Time;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.matchers.JUnitMatchers.*;

public class DurationTest
{
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

    @Test
    public void shouldComputeDurationBetweenTwoTimes()
    {
        // Given
        Time time1 = Time.fromNano( 100 );
        Time time2 = Time.fromNano( 200 );

        // When
        Duration duration = Duration.durationBetween( time1, time2 );

        // Then
        assertThat( duration, equalTo( Duration.fromNano( 100 ) ) );
    }
}
