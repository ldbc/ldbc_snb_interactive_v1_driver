package com.yahoo.ycsb.graph.workloads;

import java.util.HashMap;

import com.google.common.collect.Range;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.RandomByteIterator;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.graph.generator.Distribution;
import com.yahoo.ycsb.graph.generator.ExponentialGenerator;
import com.yahoo.ycsb.graph.generator.Generator;
import com.yahoo.ycsb.graph.generator.GeneratorFactory;
import com.yahoo.ycsb.graph.generator.exceptions.GeneratorException;

public class WorkloadUtils
{
    // Build map for updating the values of all fields
    public static HashMap<String, ByteIterator> buildAllValues( int fieldCount,
            Generator<Integer> valueLengthGenerator, String fieldNamePrefix ) throws WorkloadException
    {
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        for ( int i = 0; i < fieldCount; i++ )
        {
            String fieldname = fieldNamePrefix + i;
            ByteIterator data = new RandomByteIterator( valueLengthGenerator.next() );
            values.put( fieldname, data );
        }
        return values;
    }

    // Build map for updating the values of only one, random, field
    public static HashMap<String, ByteIterator> buildOneValue( Generator<Integer> keyNumberGenerator,
            Generator<Integer> valueLengthGenerator, String keyNamePrefix ) throws WorkloadException
    {
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        String fieldname = keyNamePrefix + keyNumberGenerator.next();
        ByteIterator data = new RandomByteIterator( valueLengthGenerator.next() );
        values.put( fieldname, data );
        return values;
    }

    // generate database key name from key number
    public static String keyNumberToKeyName( long keyNumber, boolean hashKeyNumber, String keyNamePrefix )
    {
        if ( hashKeyNumber )
        {
            keyNumber = Utils.hash( keyNumber );
        }
        return keyNamePrefix + keyNumber;
    }

    // TODO test / or remove entirely
    // Generate the next random keyNumber
    // NOTES:
    // Not sure how exactly this works
    // Seems to generate random numbers UNTIL it finds one matching a given
    // criteria
    // Seems inefficient and generally a bad approach
    public static long nextKeyNumber( Generator keyChooser, Generator<Integer> keySequence ) throws WorkloadException
    {
        // TODO lots of casting here, sign of bad code
        long keyNumber;
        if ( keyChooser instanceof ExponentialGenerator )
        {
            do
            {
                keyNumber = keySequence.last() - ( (Generator<Long>) keyChooser ).next();
            }
            while ( keyNumber < 0 );
        }
        else
        {
            do
            {
                keyNumber = (Long) keyChooser.next();
            }
            while ( keyNumber > keySequence.last() );
        }
        return keyNumber;
    }

    public static Generator buildFieldLengthGenerator( Distribution distribution, Range<Integer> range,
            String histogramFilePath ) throws WorkloadException
    {
        switch ( distribution )
        {
        case CONSTANT:
            return new GeneratorFactory().buildConstantIntegerGenerator( range.upperEndpoint() );

        case UNIFORM:
            return new GeneratorFactory().buildUniformIntegerGenerator( range );

        case ZIPFIAN:
            return new GeneratorFactory().buildZipfianIntegerGenerator( range );

        case HISTOGRAM:
            try
            {
                return new GeneratorFactory().buildHistogramIntegerGenerator( histogramFilePath );
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
