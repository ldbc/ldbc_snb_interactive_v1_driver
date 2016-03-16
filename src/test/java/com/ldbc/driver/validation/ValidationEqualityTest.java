package com.ldbc.driver.validation;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ValidationEqualityTest
{
    @Test
    public void shouldPassWhenVeryLargeDoublesAreEqual()
    {
        // Given
        double actualResult = Double.MAX_VALUE;
        double expectedResult = Double.MAX_VALUE;

        // Then
        assertTrue( ValidationEquality.doubleEquals( actualResult, expectedResult ) );
    }

    @Test
    public void shouldPassWhenVerySmallDoublesAreEqual()
    {
        // Given
        double actualResult = Double.MIN_VALUE;
        double expectedResult = Double.MIN_VALUE;

        // Then
        assertTrue( ValidationEquality.doubleEquals( actualResult, expectedResult ) );
    }

    @Test
    public void shouldPassWhenSmallNumberEqualToDecimalPlaces_10()
    {
        // Given
        double actualResult = 0.12345678901;
        double expectedResult = 0.12345678902;

        // Then
        assertTrue( ValidationEquality.doubleEquals( actualResult, expectedResult ) );
    }

    @Test
    public void shouldPassWhenBigNumberEqualToDecimalPlaces_10()
    {
        // Given
        double actualResult = 123456789.12345678901;
        double expectedResult = 123456789.12345678902;

        // Then
        assertTrue( ValidationEquality.doubleEquals( actualResult, expectedResult ) );
    }

    @Test
    public void shouldFailWhenSmallNumberEqualToOnlyDecimalPlaces_9()
    {
        // Given
        double actualResult = 0.1234567890;
        double expectedResult = 0.1234567891;

        // Then
        assertFalse( ValidationEquality.doubleEquals( actualResult, expectedResult ) );
    }
}
