package com.ldbc.driver.temporal;

import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class TimeTest {
    @Ignore
    @Test
    public void considerAddingTemporalConverterClassToRemoveStaticFromNanoEtcMethods() {
        assertThat(true, is(false));
    }

    @Ignore
    @Test
    public void createTestClassForTimeSourceToo() {
        assertThat(true, is(false));
    }

    @Ignore
    @Test
    public void addTestToFindMaximumTimeThatTimeCanStoreRememberingThatEverythingIsStoredAsNanoSeconds() {
        assertThat(true, is(false));
    }

    @Test
    public void shouldThrowExceptionIfOverflowDuringUnitConversion() {
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
        try {
            Time.fromMinutes(validMinutes);
        } catch (TemporalException e) {
            exceptionThrownOnValidValue = true;
        }
        try {
            Time.fromMinutes(invalidMinutes);
        } catch (TemporalException e) {
            exceptionThrownOnInvalidValue = true;
        }

        // Then
        assertThat(exceptionThrownOnValidValue, is(false));
        assertThat(exceptionThrownOnInvalidValue, is(true));
    }

    @Test
    public void shouldEqualWhenSame() {
        // Given
        Time time1 = Time.fromNano(1);
        Time time2 = Time.fromNano(1);

        // When

        // Then
        assertThat(time1, equalTo(time2));
    }

    @Test
    public void shouldNotEqualWhenDifferent() {
        // Given
        Time time1 = Time.fromNano(1);
        Time time2 = Time.fromNano(2);

        // When

        // Then
        assertThat(time1, not(equalTo(time2)));
    }

    @Test
    public void shouldBeSummable() {
        // Given
        Time time1 = Time.fromNano(1);
        Duration duration1 = Duration.fromNano(1);

        // When
        Time time2 = time1.plus(duration1);

        // Then
        assertThat(time2.asNano(), is(2l));
    }

    @Test
    public void shouldBeSubtractable() {
        // Given
        Time time1 = Time.fromNano(1);
        Duration duration1 = Duration.fromNano(1);

        // When
        Time time2 = time1.minus(duration1);

        // Then
        assertThat(time2.asNano(), is(0l));
    }

    @Test
    public void shouldConvertableBetweenUnits() {
        // Given
        Time time1 = Time.fromMilli(1);

        // When
        Time time2 = Time.fromNano(time1.asNano());

        // Then
        assertThat(time1.asNano(), equalTo(time2.asNano()));
    }

    @Test
    public void shouldCorrectlyCalculateTimeDifference() {
        // Given
        Time time1000 = Time.fromMilli(1000);
        Time time2000 = Time.fromMilli(2000);
        Duration duration999 = Duration.fromMilli(999);
        Duration duration1000 = Duration.fromMilli(1000);
        Duration duration1001 = Duration.fromMilli(1001);

        // When
        Duration time2000GreaterThanTime1000By = time2000.greaterBy(time1000);
        Duration time1000GreaterThanTime2000By = time1000.greaterBy(time2000);
        Duration time1000LessThanTime2000By = time1000.lessBy(time2000);
        Duration time2000LessThanTime1000By = time2000.lessBy(time1000);

        // Then
        assertThat(time2000GreaterThanTime1000By, is(Duration.fromMilli(1000)));
        assertThat(time1000GreaterThanTime2000By, is(Duration.fromMilli(-1000)));
        assertThat(time1000LessThanTime2000By, is(Duration.fromMilli(1000)));
        assertThat(time2000LessThanTime1000By, is(Duration.fromMilli(-1000)));

        assertThat(time2000GreaterThanTime1000By.gt(duration999), is(true));
        assertThat(time2000GreaterThanTime1000By.gt(duration1000), is(false));
        assertThat(time2000GreaterThanTime1000By.gt(duration1001), is(false));

        assertThat(time1000LessThanTime2000By.gt(duration999), is(true));
        assertThat(time1000LessThanTime2000By.gt(duration1000), is(false));
        assertThat(time1000LessThanTime2000By.gt(duration1001), is(false));
    }
}
