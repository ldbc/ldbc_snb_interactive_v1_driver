package com.ldbc.driver.util;

import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorBuilder;
import com.ldbc.driver.generator.wrapper.TimeFromMilliSecondsGeneratorWrapper;
import com.ldbc.driver.util.temporal.Duration;
import com.ldbc.driver.util.temporal.Time;

public class GeneratorUtils
{
    public static Generator<Time> randomTimeGeneratorFromNow( GeneratorBuilder generatorBuilder )
    {
        Duration minMsBetweenOperations = Duration.fromMilli( 1 );
        Duration maxMsBetweenOperations = Duration.fromMilli( 100 );
        Time aLittleBitAfterNow = Time.now().plus( Duration.fromSeconds( 2 ) );
        Time startTime = aLittleBitAfterNow;
        return randomTimeGeneratorFromNow( generatorBuilder, startTime, minMsBetweenOperations, maxMsBetweenOperations );
    }

    public static Generator<Time> randomTimeGeneratorFromNow( GeneratorBuilder generatorBuilder, Time startTime,
            Duration minIncrement, Duration maxIncrement )
    {
        Generator<Long> incrementTimeByGenerator = generatorBuilder.uniformNumberGenerator( minIncrement.asMilli(),
                maxIncrement.asMilli() ).build();
        Generator<Long> startTimeMilliSecondsGenerator = generatorBuilder.incrementingGenerator( startTime.asMilli(),
                incrementTimeByGenerator ).build();
        return new TimeFromMilliSecondsGeneratorWrapper( startTimeMilliSecondsGenerator );
    }

    public static Generator<Time> constantTimeGeneratorFromNow( GeneratorBuilder generatorBuilder, Time startTime,
            Duration increment )
    {
        Generator<Long> startTimeMilliSecondsGenerator = generatorBuilder.incrementingGenerator( startTime.asMilli(),
                increment.asMilli() ).build();
        return new TimeFromMilliSecondsGeneratorWrapper( startTimeMilliSecondsGenerator );
    }
}
