package com.ldbc.generator;

import org.apache.commons.math3.random.RandomDataGenerator;


/**
 * Produces a sequence of longs according to an exponential distribution.
 * Smaller intervals are more frequent than larger ones, and there is no bound
 * on the length of an interval.
 * 
 * gamma: mean rate events occur. 1/gamma: half life - average interval length
 */
public class BoundedRangeExponentialNumberGenerator extends Generator<Long>
{
    // % of readings within most recent exponential.frac portion of dataset
    public static final String EXPONENTIAL_PERCENTILE = "exponential.percentile";
    public static final String EXPONENTIAL_PERCENTILE_DEFAULT = "95";

    // Fraction of the dataset accessed exponential.percentile of the time
    public static final String EXPONENTIAL_FRAC = "exponential.frac";
    public static final String EXPONENTIAL_FRAC_DEFAULT = "0.8571428571"; // 1/7

    private final Integer MAX_ITERATIONS = 1000;
    private final MinMaxGeneratorWrapper<Long> lowerBoundGenerator;
    private final MinMaxGeneratorWrapper<Long> upperBoundGenerator;

    // Exponential constant
    private double gamma;

    public BoundedRangeExponentialNumberGenerator( RandomDataGenerator random, double mean,
            MinMaxGeneratorWrapper<Long> lowerBoundGenerator, MinMaxGeneratorWrapper<Long> upperBoundGenerator )
    {
        this( random, lowerBoundGenerator, upperBoundGenerator, calculateGamma( mean ) );
    }

    public BoundedRangeExponentialNumberGenerator( RandomDataGenerator random, double percentile, double range,
            MinMaxGeneratorWrapper<Long> lowerBoundGenerator, MinMaxGeneratorWrapper<Long> upperBoundGenerator )
    {
        this( random, lowerBoundGenerator, upperBoundGenerator, calculateGamma( percentile, range ) );
    }

    private BoundedRangeExponentialNumberGenerator( RandomDataGenerator random,
            MinMaxGeneratorWrapper<Long> lowerBoundGenerator, MinMaxGeneratorWrapper<Long> upperBoundGenerator,
            double gamma )
    {
        super( random );
        this.gamma = gamma;
        this.lowerBoundGenerator = lowerBoundGenerator;
        this.upperBoundGenerator = upperBoundGenerator;
    }

    private static double calculateGamma( double mean )
    {
        return 1.0 / mean;
    }

    private static double calculateGamma( double percentile, double range )
    {
        return -Math.log( 1.0 - percentile / 100.0 ) / range;
    }

    @Override
    protected Long doNext() throws GeneratorException
    {
        if ( null == lowerBoundGenerator || null == upperBoundGenerator )
        {
            return doNextExponential();
        }
        else
        {
            return doBoundedNextExponential();
        }
    }

    protected Long doBoundedNextExponential()
    {
        Long next;
        for ( int i = 0; i < MAX_ITERATIONS; i++ )
        {
            next = doNextExponential();
            if ( lowerBoundGenerator.getMin() <= next && next <= upperBoundGenerator.getMax() )
            {
                return next;
            }
        }
        throw new GeneratorException( String.format( "Random in-range number not be found within maxIterations[%s]",
                MAX_ITERATIONS ) );
    }

    protected Long doNextExponential()
    {
        return (long) ( -Math.log( getRandom().nextUniform( 0, 1 ) ) / gamma );
    }
}
