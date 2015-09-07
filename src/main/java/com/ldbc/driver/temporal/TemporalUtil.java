package com.ldbc.driver.temporal;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class TemporalUtil
{
    private final SimpleDateFormat timeFormat;
    private final SimpleDateFormat dateTimeFormat;

    public TemporalUtil()
    {
        timeFormat = new SimpleDateFormat( "HH:mm:ss.SSS" );
        timeFormat.setTimeZone( TimeZone.getTimeZone( "GMT" ) );
        dateTimeFormat = new SimpleDateFormat( "yyyy-MM-dd - HH:mm:ss.SSS" );
        dateTimeFormat.setTimeZone( TimeZone.getTimeZone( "GMT" ) );
    }

    public String milliTimeToTimeString( long timeAsMilli )
    {
        return timeFormat.format( new Date( timeAsMilli ) );
    }

    public String milliTimeToDateTimeString( long timeAsMilli )
    {
        return dateTimeFormat.format( new Date( timeAsMilli ) );
    }

    public String milliDurationToString( long durationAsMilli )
    {
        return nanoDurationToString( TimeUnit.MILLISECONDS.toNanos( durationAsMilli ), false );
    }

    public String nanoDurationToString( long durationAsNano )
    {
        return nanoDurationToString( durationAsNano, true );
    }

    private String nanoDurationToString( long durationAsNano, boolean detailed )
    {
        long h = TimeUnit.NANOSECONDS.toHours( durationAsNano );
        if ( h > 0 )
        {
            long m = TimeUnit.NANOSECONDS.toMinutes( durationAsNano ) - TimeUnit.HOURS.toMinutes( h );
            long s = TimeUnit.NANOSECONDS.toSeconds( durationAsNano ) - TimeUnit.HOURS.toSeconds( h ) -
                     TimeUnit.MINUTES.toSeconds( m );
            long ms = TimeUnit.NANOSECONDS.toMillis( durationAsNano ) - TimeUnit.HOURS.toMillis( h ) -
                      TimeUnit.MINUTES.toMillis( m ) - TimeUnit.SECONDS.toMillis( s );
            if ( detailed )
            {
                long us = TimeUnit.NANOSECONDS.toMicros( durationAsNano ) - TimeUnit.HOURS.toMicros( h ) -
                          TimeUnit.MINUTES.toMicros( m ) - TimeUnit.SECONDS.toMicros( s ) -
                          TimeUnit.MILLISECONDS.toMicros( ms );
                return format( "%02d:%02d:%02d.%03d.%03d (h:m:s.ms.us)", h, m, s, ms, us );
            }
            else
            {
                return format( "%02d:%02d:%02d.%03d (h:m:s.ms)", h, m, s, ms );
            }
        }
        else
        {
            long m = TimeUnit.NANOSECONDS.toMinutes( durationAsNano );
            long s = TimeUnit.NANOSECONDS.toSeconds( durationAsNano ) - TimeUnit.MINUTES.toSeconds( m );
            long ms = TimeUnit.NANOSECONDS.toMillis( durationAsNano ) - TimeUnit.MINUTES.toMillis( m ) -
                      TimeUnit.SECONDS.toMillis( s );
            if ( detailed )
            {
                long us = TimeUnit.NANOSECONDS.toMicros( durationAsNano ) - TimeUnit.MINUTES.toMicros( m ) -
                          TimeUnit.SECONDS.toMicros( s ) - TimeUnit.MILLISECONDS.toMicros( ms );
                return format( "%02d:%02d.%03d.%03d (m:s.ms.us)", m, s, ms, us );
            }
            else
            {
                return format( "%02d:%02d.%03d (m:s.ms)", m, s, ms );
            }
        }
    }

    public String abbreviatedTimeUnit( TimeUnit unit )
    {
        switch ( unit )
        {
        case NANOSECONDS:
        {
            return "ns";
        }
        case MICROSECONDS:
        {
            return "us";
        }
        case MILLISECONDS:
        {
            return "ms";
        }
        case SECONDS:
        {
            return "s";
        }
        case MINUTES:
        {
            return "m";
        }
        case HOURS:
        {
            return "h";
        }
        case DAYS:
        {
            return "d";
        }
        default:
        {
            return unit.name();
        }
        }
    }
}
