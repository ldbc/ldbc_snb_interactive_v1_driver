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
        long minMsBetweenOperations = 1l;
        long maxMsBetweenOperations = 100l;
        Time aLittleBitAfterNow = Time.now().plus( Duration.fromSeconds( 2 ) );
        Time startTime = aLittleBitAfterNow;
        return randomTimeGeneratorFromNow( generatorBuilder, startTime, minMsBetweenOperations, maxMsBetweenOperations );
    }

    public static Generator<Time> randomTimeGeneratorFromNow( GeneratorBuilder generatorBuilder, Time startTime,
            long minMsIncrement, long maxMsIncrement )
    {
        Generator<Long> incrementTimeByGenerator = generatorBuilder.uniformNumberGenerator( minMsIncrement,
                maxMsIncrement ).build();
        Generator<Long> startTimeMilliSecondsGenerator = generatorBuilder.incrementingGenerator( startTime.asMilli(),
                incrementTimeByGenerator ).build();
        return new TimeFromMilliSecondsGeneratorWrapper( startTimeMilliSecondsGenerator );
    }
}
