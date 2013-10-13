package com.ldbc.driver.util;

import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.wrapper.TimeFromMilliSecondsGeneratorWrapper;
import com.ldbc.driver.util.temporal.Duration;
import com.ldbc.driver.util.temporal.Time;

public class GeneratorUtils
{
    public static Generator<Time> randomTimeGeneratorFromNow( GeneratorFactory generators )
    {
        Duration minMsBetweenOperations = Duration.fromMilli( 1 );
        Duration maxMsBetweenOperations = Duration.fromMilli( 100 );
        Time aLittleBitAfterNow = Time.now().plus( Duration.fromSeconds( 2 ) );
        Time startTime = aLittleBitAfterNow;
        return randomTimeGeneratorFromNow( generators, startTime, minMsBetweenOperations, maxMsBetweenOperations );
    }

    public static Generator<Time> randomTimeGeneratorFromNow( GeneratorFactory generators, Time startTime,
            Duration minIncrement, Duration maxIncrement )
    {
        Generator<Long> incrementTimeByGenerator = generators.uniformNumberGenerator( minIncrement.asMilli(),
                maxIncrement.asMilli() );
        Generator<Long> startTimeMilliSecondsGenerator = generators.incrementingGenerator( startTime.asMilli(),
                incrementTimeByGenerator );
        return new TimeFromMilliSecondsGeneratorWrapper( startTimeMilliSecondsGenerator );
    }

    public static Generator<Time> constantTimeGeneratorFromNow( GeneratorFactory generators, Time startTime,
            Duration increment )
    {
        Generator<Long> startTimeMilliSecondsGenerator = generators.incrementingGenerator( startTime.asMilli(),
                increment.asMilli() );
        return new TimeFromMilliSecondsGeneratorWrapper( startTimeMilliSecondsGenerator );
    }
}
