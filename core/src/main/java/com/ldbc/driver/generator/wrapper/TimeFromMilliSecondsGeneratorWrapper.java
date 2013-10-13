package com.ldbc.driver.generator.wrapper;

import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.generator.MappingGenerator;
import com.ldbc.driver.util.Function;
import com.ldbc.driver.util.temporal.Time;

public class TimeFromMilliSecondsGeneratorWrapper extends Generator<Time>
{
    private final Generator<Time> timeGenerator;

    public TimeFromMilliSecondsGeneratorWrapper( Generator<Long> milliSecondsGenerator )
    {
        Function<Long, Time> timeFromNanoFun = new Function<Long, Time>()
        {
            @Override
            public Time apply( Long fromMilli )
            {
                return Time.fromMilli( fromMilli );
            }
        };
        this.timeGenerator = new MappingGenerator<Long, Time>( milliSecondsGenerator, timeFromNanoFun );
    }

    @Override
    protected Time doNext() throws GeneratorException
    {
        return ( timeGenerator.hasNext() ) ? timeGenerator.next() : null;
    }
}
