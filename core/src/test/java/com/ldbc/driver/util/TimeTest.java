package com.ldbc.driver.util;

import org.junit.Test;

import com.ldbc.driver.util.temporal.Duration;
import com.ldbc.driver.util.temporal.Time;

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

    @Test
    public void shouldReturnNow()
    {
        // Given
        Time now = Time.now();
        long nowMilli = System.currentTimeMillis();

        // When
        long difference = Math.abs( now.asMilli() - nowMilli );

        // Then
        assertThat( difference < 10, is( true ) );
    }

    @Test
    public void shouldCorrectlyCalculateTimeDifference()
    {
        // Given
        Time time1000 = Time.fromMilli( 1000 );
        Time time2000 = Time.fromMilli( 2000 );
        Duration duration999 = Duration.fromMilli( 999 );
        Duration duration1000 = Duration.fromMilli( 1000 );
        Duration duration1001 = Duration.fromMilli( 1001 );

        // When
        Duration time2000GreaterThanTime1000By = time2000.greaterBy( time1000 );
        Duration time1000GreaterThanTime2000By = time1000.greaterBy( time2000 );
        Duration time1000LessThanTime2000By = time1000.lessBy( time2000 );
        Duration time2000LessThanTime1000By = time2000.lessBy( time1000 );

        // Then
        assertThat( time2000GreaterThanTime1000By, is( Duration.fromMilli( 1000 ) ) );
        assertThat( time1000GreaterThanTime2000By, is( Duration.fromMilli( -1000 ) ) );
        assertThat( time1000LessThanTime2000By, is( Duration.fromMilli( 1000 ) ) );
        assertThat( time2000LessThanTime1000By, is( Duration.fromMilli( -1000 ) ) );

        assertThat( time2000GreaterThanTime1000By.greatThan( duration999 ), is( true ) );
        assertThat( time2000GreaterThanTime1000By.greatThan( duration1000 ), is( false ) );
        assertThat( time2000GreaterThanTime1000By.greatThan( duration1001 ), is( false ) );

        assertThat( time1000LessThanTime2000By.greatThan( duration999 ), is( true ) );
        assertThat( time1000LessThanTime2000By.greatThan( duration1000 ), is( false ) );
        assertThat( time1000LessThanTime2000By.greatThan( duration1001 ), is( false ) );
    }
}
