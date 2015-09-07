package com.ldbc.driver.util;

import com.ldbc.driver.generator.GeneratorException;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.random.RandomDataGenerator;

import static java.lang.String.format;

public abstract class NumberHelper<T extends Number>
{
    private static final NumberHelper<Integer> integerNumberHelper = new IntegerNumberHelper();
    private static final NumberHelper<Long> longNumberHelper = new LongNumberHelper();
    private static final NumberHelper<Double> doubleNumberHelper = new DoubleNumberHelper();

    public static <T1 extends Number> NumberHelper<T1> createNumberHelper( Class<?> type )
    {
        /* 
         * Supported: Double, Integer, Long
         */
        if ( type.isAssignableFrom( Integer.class ) )
        { return (NumberHelper<T1>) integerNumberHelper; }
        if ( type.isAssignableFrom( Long.class ) )
        { return (NumberHelper<T1>) longNumberHelper; }
        if ( type.isAssignableFrom( Double.class ) )
        { return (NumberHelper<T1>) doubleNumberHelper; }
        /*  
         * Not supported: Byte, Float, Short, AtomicInteger, AtomicLong, BigDecimal, BigInteger
         */
        throw new GeneratorException( format( "%s not supported. Only supports: Double, Integer, Long",
                type.getName() ) );
    }

    public static <T1 extends Number> boolean withinTolerance( T1 a, T1 b, Number tolerance )
    {
        Double difference = Math.abs( (a.doubleValue() - b.doubleValue()) );
        return difference <= tolerance.doubleValue();
    }

    public final T sum( Iterable<T> ts )
    {
        T sum = zero();
        for ( T t : ts )
        {
            sum = sum( sum, t );
        }
        return sum;
    }

    public final T one()
    {
        return inc( zero() );
    }

    public abstract T cast( Number n );

    public abstract T inc( T a );

    public abstract T zero();

    public abstract T sum( T a, T b );

    public abstract T sub( T a, T b );

    public abstract T div( T a, T b );

    public abstract T min();

    public abstract T max();

    public abstract T round( Number number );

    public abstract T uniform( RandomDataGenerator random, T min, T max );

    public abstract T exponential( ExponentialDistribution exponentialDistribution );

    public abstract boolean lt( T a, T b );

    public abstract boolean lte( T a, T b );

    public abstract boolean gt( T a, T b );

    public abstract boolean gte( T a, T b );

    private static class IntegerNumberHelper extends NumberHelper<Integer>
    {
        @Override
        public Integer cast( Number n )
        {
            return n.intValue();
        }

        @Override
        public Integer zero()
        {
            return 0;
        }

        @Override
        public Integer inc( Integer a )
        {
            return a + 1;
        }

        @Override
        public Integer sum( Integer a, Integer b )
        {
            return a + b;
        }

        @Override
        public Integer sub( Integer a, Integer b )
        {
            return a - b;
        }

        @Override
        public Integer div( Integer a, Integer b )
        {
            return a / b;
        }

        @Override
        public Integer min()
        {
            return Integer.MIN_VALUE;
        }

        @Override
        public Integer max()
        {
            return Integer.MAX_VALUE;
        }

        @Override
        public Integer round( Number number )
        {
            return (int) Math.round( number.doubleValue() );
        }

        @Override
        public Integer uniform( RandomDataGenerator random, Integer min, Integer max )
        {
            if ( min == max )
            {
                return min;
            }
            return random.nextInt( min, max );
        }

        @Override
        public Integer exponential( ExponentialDistribution exponentialDistribution )
        {
            return new Double( exponentialDistribution.sample() ).intValue();
        }

        @Override
        public boolean lt( Integer a, Integer b )
        {
            return a < b;
        }

        @Override
        public boolean gt( Integer a, Integer b )
        {
            return a > b;
        }

        @Override
        public boolean lte( Integer a, Integer b )
        {
            return a <= b;
        }

        @Override
        public boolean gte( Integer a, Integer b )
        {
            return a >= b;
        }
    }

    private static class LongNumberHelper extends NumberHelper<Long>
    {
        @Override
        public Long cast( Number n )
        {
            return n.longValue();
        }

        @Override
        public Long zero()
        {
            return 0l;
        }

        @Override
        public Long inc( Long a )
        {
            return a + 1;
        }

        @Override
        public Long sum( Long a, Long b )
        {
            return a + b;
        }

        @Override
        public Long sub( Long a, Long b )
        {
            return a - b;
        }

        @Override
        public Long div( Long a, Long b )
        {
            return a / b;
        }

        @Override
        public Long min()
        {
            return Long.MIN_VALUE;
        }

        @Override
        public Long max()
        {
            return Long.MAX_VALUE;
        }

        @Override
        public Long round( Number number )
        {
            return Math.round( number.doubleValue() );
        }

        @Override
        public Long uniform( RandomDataGenerator random, Long min, Long max )
        {
            if ( min == max )
            {
                return min;
            }
            return random.nextLong( min, max );
        }

        @Override
        public Long exponential( ExponentialDistribution exponentialDistribution )
        {
            return new Double( exponentialDistribution.sample() ).longValue();
        }

        @Override
        public boolean lt( Long a, Long b )
        {
            return a < b;
        }

        @Override
        public boolean gt( Long a, Long b )
        {
            return a > b;
        }

        @Override
        public boolean lte( Long a, Long b )
        {
            return a <= b;
        }

        @Override
        public boolean gte( Long a, Long b )
        {
            return a >= b;
        }
    }

    private static class DoubleNumberHelper extends NumberHelper<Double>
    {
        @Override
        public Double cast( Number n )
        {
            return n.doubleValue();
        }

        @Override
        public Double zero()
        {
            return 0d;
        }

        @Override
        public Double inc( Double a )
        {
            return a + 1;
        }

        @Override
        public Double sum( Double a, Double b )
        {
            return a + b;
        }

        @Override
        public Double sub( Double a, Double b )
        {
            return a - b;
        }

        @Override
        public Double div( Double a, Double b )
        {
            return a / b;
        }

        @Override
        public Double min()
        {
            return Double.MIN_VALUE;
        }

        @Override
        public Double max()
        {
            return Double.MAX_VALUE;
        }

        @Override
        public Double round( Number number )
        {
            return number.doubleValue();
        }

        @Override
        public Double uniform( RandomDataGenerator random, Double min, Double max )
        {
            if ( min == max )
            {
                return min;
            }
            return random.nextUniform( min, max, true );
        }

        @Override
        public Double exponential( ExponentialDistribution exponentialDistribution )
        {
            return exponentialDistribution.sample();
        }

        @Override
        public boolean lt( Double a, Double b )
        {
            return a < b;
        }

        @Override
        public boolean gt( Double a, Double b )
        {
            return a > b;
        }

        @Override
        public boolean lte( Double a, Double b )
        {
            return a <= b;
        }

        @Override
        public boolean gte( Double a, Double b )
        {
            return a >= b;
        }
    }
}
