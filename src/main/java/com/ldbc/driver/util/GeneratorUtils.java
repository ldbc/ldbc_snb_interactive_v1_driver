package com.ldbc.driver.util;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.MappingGenerator;
import com.ldbc.driver.generator.OrderedMultiGenerator;
import com.ldbc.driver.util.temporal.Duration;
import com.ldbc.driver.util.temporal.Time;

public class GeneratorUtils
{
    public static Iterator<Time> randomIncrementStartTimeGenerator( GeneratorFactory generators, Time startTime,
            Duration minIncrement, Duration maxIncrement )
    {
        Iterator<Long> incrementTimeByGenerator = generators.uniform( minIncrement.asMilli(),
                maxIncrement.asMilli() );
        Iterator<Long> startTimeMilliSecondsGenerator = generators.incrementing( startTime.asMilli(),
                incrementTimeByGenerator );
        return timeFromMilliSeconds( startTimeMilliSecondsGenerator );
    }

    public static Iterator<Time> constantIncrementStartTimeGenerator( GeneratorFactory generators, Time startTime,
            Duration increment )
    {
        Iterator<Long> startTimeMilliSecondsGenerator = generators.incrementing( startTime.asMilli(),
                increment.asMilli() );
        return timeFromMilliSeconds( startTimeMilliSecondsGenerator );
    }

    public static Iterator<Time> timeFromMilliSeconds( Iterator<Long> milliSecondsGenerator )
    {
        Function1<Long, Time> timeFromNanoFun = new Function1<Long, Time>()
        {
            @Override
            public Time apply( Long fromMilli )
            {
                return Time.fromMilli( fromMilli );
            }
        };
        return new MappingGenerator<Long, Time>( milliSecondsGenerator, timeFromNanoFun );
    }

    public static Iterator<Operation<?>> orderOperationsByScheduledStartTime( int lookaheadDistance,
            Iterator<Operation<?>>... operationGenerators )
    {
        return new OrderedMultiGenerator<Operation<?>>( new Comparator<Operation<?>>()
        {
            @Override
            public int compare( Operation<?> o1, Operation<?> o2 )
            {
                return o1.scheduledStartTime().compareTo( o2.scheduledStartTime() );
            }
        }, lookaheadDistance, operationGenerators );
    }

    public static <T1> Iterator<T1> includeOnly( Iterator<T1> generator, T1... includedItems )
    {
        return Iterators.filter( generator, new IncludeOnlyPredicate<T1>( includedItems ) );
    }

    public static <T1> Iterator<T1> excludeAll( Iterator<T1> generator, T1... excludedItems )
    {
        return Iterators.filter( generator, new ExcludeAllPredicate<T1>( excludedItems ) );
    }

    private static class IncludeOnlyPredicate<T1> implements Predicate<T1>
    {
        private final Set<T1> includedItems;

        private IncludeOnlyPredicate( T1... includedItems )
        {
            this.includedItems = new HashSet<T1>( Arrays.asList( includedItems ) );
        }

        @Override
        public boolean apply( T1 input )
        {
            return true == includedItems.contains( input );
        }
    }

    private static class ExcludeAllPredicate<T1> implements Predicate<T1>
    {
        private final Set<T1> excludedItems;

        private ExcludeAllPredicate( T1... excludedItems )
        {
            this.excludedItems = new HashSet<T1>( Arrays.asList( excludedItems ) );
        }

        @Override
        public boolean apply( T1 input )
        {
            return false == excludedItems.contains( input );
        }
    }

}
