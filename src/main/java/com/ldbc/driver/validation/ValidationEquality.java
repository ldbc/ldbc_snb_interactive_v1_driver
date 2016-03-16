package com.ldbc.driver.validation;

import java.math.RoundingMode;
import java.text.DecimalFormat;

public class ValidationEquality
{
    private static final DecimalFormat DOUBLE_FORMAT;

    static
    {
        // compare to 10 decimal places      .1234567890
        DOUBLE_FORMAT = new DecimalFormat( "#.##########" );
        DOUBLE_FORMAT.setRoundingMode( RoundingMode.CEILING );
    }

    public static boolean doubleEquals( Double actual, Double expected )
    {
        return DOUBLE_FORMAT.format( actual ).equals( DOUBLE_FORMAT.format( expected ) );
    }
}
