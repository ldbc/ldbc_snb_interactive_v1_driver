package com.yahoo.ycsb.workloads;

import java.util.HashMap;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.google.common.collect.Range;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBRecordKey;
import com.yahoo.ycsb.RandomByteIterator;
import com.yahoo.ycsb.RandomDataGeneratorFactory;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.ExponentialGenerator;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.GeneratorException;
import com.yahoo.ycsb.generator.GeneratorFactory;

public class WorkloadUtils
{
    // TODO temp, this should be given in the constructor, remove later
    static GeneratorFactory generatorFactory = new GeneratorFactory( new RandomDataGeneratorFactory( 42l ) );

    // TODO temp, this should be given in the constructor, remove later
    static RandomDataGenerator random = new RandomDataGenerator();

    // Build map for updating the values of all fields
    public static HashMap<String, ByteIterator> buildAllValues( int fieldCount, Generator<Long> valueLengthGenerator,
            String fieldNamePrefix ) throws WorkloadException
    {
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        for ( int i = 0; i < fieldCount; i++ )
        {
            String fieldname = fieldNamePrefix + i;
            ByteIterator data = new RandomByteIterator( valueLengthGenerator.next(), random );
            values.put( fieldname, data );
        }
        return values;
    }

    // Build map for updating the values of only one, random, field
    public static HashMap<String, ByteIterator> buildOneValue( Generator<Long> keyNumberGenerator,
            Generator<Long> valueLengthGenerator, String keyNamePrefix ) throws WorkloadException
    {
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        String fieldname = keyNamePrefix + keyNumberGenerator.next();
        ByteIterator data = new RandomByteIterator( valueLengthGenerator.next(), random );
        values.put( fieldname, data );
        return values;
    }

    // TODO test / or remove entirely
    // Generate the next random keyNumber
    // NOTES:
    // Not sure how exactly this works
    // Seems to generate random numbers UNTIL it finds one matching a given
    // criteria (e.g. in range)
    // Seems inefficient and generally a bad approach
    public static DBRecordKey nextKey( Generator<Long> keyChooser, Generator<Long> keySequence ) throws WorkloadException
    {
        // TODO lots of casting here, sign of bad code
        long keyNumber;
        if ( keyChooser instanceof ExponentialGenerator )
        {
            do
            {
                keyNumber = keySequence.last() - keyChooser.next();
            }
            while ( keyNumber < 0 );
        }
        else
        {
            // TODO this bullshit isn't necessary
            // TODO just generated 0..1 double, then multiply by last()
            do
            {
                keyNumber = keyChooser.next();
            }
            while ( keyNumber > keySequence.last() );
        }
        return new DBRecordKey( keyNumber );
    }

    public static Generator<Long> buildFieldLengthGenerator( Distribution distribution, Range<Long> range,
            String histogramFilePath ) throws WorkloadException
    {
        switch ( distribution )
        {
        case CONSTANT:
            return generatorFactory.newConstantIntegerGenerator( range.upperEndpoint() );

        case UNIFORM:
            return generatorFactory.newUniformIntegerGenerator( range );

        case ZIPFIAN:
            return generatorFactory.newZipfianGenerator( range );

        case HISTOGRAM:
            try
            {
                return generatorFactory.newHistogramIntegerGenerator( histogramFilePath );
            }
            catch ( GeneratorException e )
            {
                throw new WorkloadException( "Error encountered while creating HistogramGenerator", e.getCause() );
            }

        default:
            String errMsg = String.format( "Invalid Distribution [%s], use one of the following: %s, %s, %s, %s",
                    distribution, Distribution.CONSTANT, Distribution.UNIFORM, Distribution.ZIPFIAN,
                    Distribution.HISTOGRAM );
            throw new WorkloadException( errMsg );
        }
    }
}
