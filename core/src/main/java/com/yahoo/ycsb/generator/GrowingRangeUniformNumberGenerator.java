package com.yahoo.ycsb.generator;

import org.apache.commons.math3.random.RandomDataGenerator;

public class GrowingRangeUniformNumberGenerator<T extends Number> extends Generator<T>
{
    private final T lowerBound;
    private final Generator<T> upperBoundGenerator;
    private final NextDelegate<T> nextDelegate;

    GrowingRangeUniformNumberGenerator( RandomDataGenerator random, T lowerBound, Generator<T> upperBoundGenerator )
    {
        super( random );
        this.lowerBound = lowerBound;
        this.upperBoundGenerator = upperBoundGenerator;
        this.nextDelegate = createNextDelegate( lowerBound.getClass() );
    }

    @Override
    protected T doNext() throws GeneratorException
    {
        return nextDelegate.next( lowerBound, upperBoundGenerator.last(), getRandom() );
    }

    private NextDelegate<T> createNextDelegate( Class<? extends Number> type )
    {
        /* 
         * Supported: Double, Integer, Long
         */
        if ( type.getClass().isAssignableFrom( Integer.class ) ) return (NextDelegate<T>) new IntegerDelegate();
        if ( type.getClass().isAssignableFrom( Long.class ) ) return (NextDelegate<T>) new LongDelegate();
        if ( type.getClass().isAssignableFrom( Double.class ) ) return (NextDelegate<T>) new DoubleDelegate();
        /*  
         * Not supported: Byte, Float, Short, AtomicInteger, AtomicLong, BigDecimal, BigInteger
         */
        throw new GeneratorException( String.format( "%s not supported. Only supports: Double, Integer, Long",
                type.getName() ) );
    }

    private abstract class NextDelegate<T1 extends Number>
    {
        private T1 max;

        public final T1 next( T1 lb, T1 ub, RandomDataGenerator random )
        {
            return doNext( lb, max( ub, max ), random );
        }

        public abstract T1 doNext( T1 lb, T1 ub, RandomDataGenerator random );

        protected abstract T1 max( T1 value1, T1 value2 );
    }

    private class IntegerDelegate extends NextDelegate<Integer>
    {
        @Override
        public Integer doNext( Integer lb, Integer ub, RandomDataGenerator random )
        {
            return (int) Math.round( ( lb - ub ) * random.nextUniform( lb, ub ) ) + lb;
        }

        @Override
        protected Integer max( Integer value1, Integer value2 )
        {
            return ( value1 > value2 ) ? value1 : value2;
        }
    }

    private class LongDelegate extends NextDelegate<Long>
    {
        @Override
        public Long doNext( Long lb, Long ub, RandomDataGenerator random )
        {
            return Math.round( ( ub - lb ) * random.nextUniform( lb, ub ) ) + lb;
        }

        @Override
        protected Long max( Long value1, Long value2 )
        {
            return ( value1 > value2 ) ? value1 : value2;
        }
    }

    private class DoubleDelegate extends NextDelegate<Double>
    {
        @Override
        public Double doNext( Double lb, Double ub, RandomDataGenerator random )
        {
            return ( ( ub - lb ) * random.nextUniform( lb, ub ) ) + lb;
        }

        @Override
        protected Double max( Double value1, Double value2 )
        {
            return ( value1 > value2 ) ? value1 : value2;
        }
    }
}
