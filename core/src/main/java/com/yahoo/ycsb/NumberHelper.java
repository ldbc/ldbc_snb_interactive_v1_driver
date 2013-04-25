package com.yahoo.ycsb;

import com.yahoo.ycsb.generator.GeneratorException;

public abstract class NumberHelper<T extends Number>
{
    public static <T1 extends Number> NumberHelper<T1> createNumberHelper( Class<?> type )
    {
        /* 
         * Supported: Double, Integer, Long
         */
        if ( type.isAssignableFrom( Integer.class ) ) return (NumberHelper<T1>) new IntegerNumberHelper();
        if ( type.isAssignableFrom( Long.class ) ) return (NumberHelper<T1>) new LongNumberHelper();
        if ( type.isAssignableFrom( Double.class ) ) return (NumberHelper<T1>) new DoubleNumberHelper();
        /*  
         * Not supported: Byte, Float, Short, AtomicInteger, AtomicLong, BigDecimal, BigInteger
         */
        throw new GeneratorException( String.format( "%s not supported. Only supports: Double, Integer, Long",
                type.getName() ) );
    }

    public static NumberHelper<Integer> createIntegerNumberHelper()
    {
        return new IntegerNumberHelper();
    }

    public static NumberHelper<Long> createLongNumberHelper()
    {
        return new LongNumberHelper();
    }

    public static NumberHelper<Double> createDoubleNumberHelper()
    {
        return new DoubleNumberHelper();
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

    public abstract T inc( T a );

    public abstract T zero();

    public final T one()
    {
        return inc( zero() );
    }

    public abstract T sum( T a, T b );

    public abstract T div( T a, T b );

    public static <T1 extends Number> boolean withinTolerance( T1 a, T1 b, Double tolerance )
    {
        Double difference = Math.abs( ( a.doubleValue() - b.doubleValue() ) );
        return difference <= tolerance;
    }

    private static class IntegerNumberHelper extends NumberHelper<Integer>
    {
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
        public Integer div( Integer a, Integer b )
        {
            return a / b;
        }
    }

    private static class LongNumberHelper extends NumberHelper<Long>
    {
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
        public Long div( Long a, Long b )
        {
            return a / b;
        }
    }

    private static class DoubleNumberHelper extends NumberHelper<Double>
    {
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
        public Double div( Double a, Double b )
        {
            return a / b;
        }
    }
}
