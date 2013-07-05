package com.ldbc.driver.util;

import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorBuilder;
import com.ldbc.driver.generator.wrapper.TimeFromMilliSecondsGeneratorWrapper;
import com.ldbc.driver.util.temporal.Duration;
import com.ldbc.driver.util.temporal.Time;

public class GeneratorUtils
{
    private static final Duration _2_SECONDS = Duration.fromSeconds( 2 );

    public static Generator<Time> randomTimeGeneratorFromNow( GeneratorBuilder generatorBuilder, long minMsIncrement,
            long maxMsIncrement )
    {
        Generator<Long> incrementTimeByGenerator = generatorBuilder.uniformNumberGenerator( minMsIncrement,
                maxMsIncrement ).build();
        Time aLittleBitAfterNow = Time.now().plus( _2_SECONDS );
        Generator<Long> startTimeMilliSecondsGenerator = generatorBuilder.incrementingGenerator(
                aLittleBitAfterNow.asMilli(), incrementTimeByGenerator ).build();
        return new TimeFromMilliSecondsGeneratorWrapper( startTimeMilliSecondsGenerator );
    }
}
