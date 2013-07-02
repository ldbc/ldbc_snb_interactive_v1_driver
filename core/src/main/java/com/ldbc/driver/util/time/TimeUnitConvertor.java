package com.ldbc.driver.util.time;

public class TimeUnitConvertor
{
    private static final long SECOND_OFFSET = 1000 * 1000 * 1000;
    private static final long MILLI_OFFSET = 1000 * 1000;
    private static final long MICRO_OFFSET = 1000;

    public static long nanoToMilli( long timeNano )
    {
        return timeNano / MILLI_OFFSET;
    }

    public static long nanoToMicro( long timeNano )
    {
        return timeNano / MICRO_OFFSET;
    }

    public static long nanoToSecond( long timeNano )
    {
        return timeNano / SECOND_OFFSET;
    }

    public static long nanoFromMilli( long timeMilli )
    {
        return timeMilli * MILLI_OFFSET;
    }

    public static long nanoFromMicro( long timeMicro )
    {
        return timeMicro * MICRO_OFFSET;
    }

    public static long nanoFromSecond( long timeSecond )
    {
        return timeSecond * SECOND_OFFSET;
    }

}
