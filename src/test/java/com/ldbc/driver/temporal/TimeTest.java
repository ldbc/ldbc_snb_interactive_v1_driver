package com.ldbc.driver.temporal;

import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class TimeTest {
    @Ignore
    @Test
    public void introduceTimeStampType() {
        // TODO TimeStamp = Time + unique long id
        // TODO serves two purposes:
        // TODO (1) gives operations unique ID
        // TODO (2) makes GCT tracking easier because TimeStamps are all unique, allows for more efficient implementations
        assertThat(true, is(false));
    }

    @Test
    public void timeAndDurationShouldBeCapableOfStoringSufficientlyLargeValuesAtNanoSecondResolution() {
        long secondAsMilli = 1000;
        long minuteAsMilli = secondAsMilli * 60;
        long hourAsMilli = minuteAsMilli * 60;
        long dayAsMilli = hourAsMilli * 24;
        long weekAsMilli = dayAsMilli * 7;
        long yearAsMilli = dayAsMilli * 365;

        Duration secondDuration = Duration.fromSeconds(1);
        Duration minuteDuration = Duration.fromMinutes(1);
        Duration hourDuration = Duration.fromMinutes(60);
        Duration dayDuration = Duration.fromMinutes(60 * 24);
        Duration weekDuration = Duration.fromMinutes(60 * 24 * 7);
        Duration yearDuration = Duration.fromMinutes(60 * 24 * 365);

        Time secondTime = Time.fromSeconds(1);
        Time minuteTime = Time.fromMinutes(1);
        Time hourTime = Time.fromMinutes(60);
        Time dayTime = Time.fromMinutes(60 * 24);
        Time weekTime = Time.fromMinutes(60 * 24 * 7);
        Time yearTime = Time.fromMinutes(60 * 24 * 365);

        assertThat(secondAsMilli, is(secondDuration.asMilli()));
        assertThat(minuteAsMilli, is(minuteDuration.asMilli()));
        assertThat(hourAsMilli, is(hourDuration.asMilli()));
        assertThat(dayAsMilli, is(dayDuration.asMilli()));
        assertThat(weekAsMilli, is(weekDuration.asMilli()));
        assertThat(yearAsMilli, is(yearDuration.asMilli()));

        assertThat(secondAsMilli, is(secondTime.asMilli()));
        assertThat(minuteAsMilli, is(minuteTime.asMilli()));
        assertThat(hourAsMilli, is(hourTime.asMilli()));
        assertThat(dayAsMilli, is(dayTime.asMilli()));
        assertThat(weekAsMilli, is(weekTime.asMilli()));
        assertThat(yearAsMilli, is(yearTime.asMilli()));

        System.out.println(String.format("Java long can hold %s DAYS with NANOSECOND resolution", Long.MAX_VALUE / dayDuration.asNano()));
        System.out.println(String.format("Java long can hold %s DAYS with MICROSECOND resolution", Long.MAX_VALUE / dayDuration.asMicro()));
        System.out.println(String.format("Java long can hold %s DAYS with MILLISECOND resolution", Long.MAX_VALUE / dayDuration.asMilli()));
        System.out.println(String.format("Java long can hold %s DAYS with SECOND resolution", Long.MAX_VALUE / dayDuration.asSeconds()));

        System.out.println(String.format("Java long can hold %s YEARS with NANOSECOND resolution", Long.MAX_VALUE / yearDuration.asNano()));
        System.out.println(String.format("Java long can hold %s YEARS with MICROSECOND resolution", Long.MAX_VALUE / yearDuration.asMicro()));
        System.out.println(String.format("Java long can hold %s YEARS with MILLISECOND resolution", Long.MAX_VALUE / yearDuration.asMilli()));
        System.out.println(String.format("Java long can hold %s YEARS with SECOND resolution", Long.MAX_VALUE / yearDuration.asSeconds()));
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
        Duration time2000GreaterThanTime1000By = time2000.durationGreaterThan(time1000);
        Duration time1000GreaterThanTime2000By = time1000.durationGreaterThan(time2000);
        Duration time1000LessThanTime2000By = time1000.durationLessThan(time2000);
        Duration time2000LessThanTime1000By = time2000.durationLessThan(time1000);

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
