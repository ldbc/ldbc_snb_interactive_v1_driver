package com.yahoo.ycsb.workloads;

import java.util.HashMap;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.yahoo.ycsb.DBRecordKey;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.GeneratorBuilder;
import com.yahoo.ycsb.generator.ycsb.ExponentialGenerator;
import com.yahoo.ycsb.util.ByteIterator;
import com.yahoo.ycsb.util.RandomByteIterator;

public class WorkloadUtils
{
    // TODO temp, this should be given in the constructor, remove later
    static RandomDataGenerator random = new RandomDataGenerator();

    // Build map for updating the values of all fields
    public static HashMap<String, ByteIterator> buildRecordWithAllFields( long fieldCount,
            Generator<Long> valueLengthGenerator, String fieldNamePrefix ) throws WorkloadException
    {
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();

        for ( long i = 0; i < fieldCount; i++ )
        {
            String fieldname = fieldNamePrefix + i;
            ByteIterator data = new RandomByteIterator( valueLengthGenerator.next(), random );
            values.put( fieldname, data );
        }

        return values;
    }

    // Build map for updating the values of only one, random, field
    public static HashMap<String, ByteIterator> buildRecordWithOneField( Generator<Long> keyNumberGenerator,
            Generator<Long> valueLengthGenerator, String keyNamePrefix ) throws WorkloadException
    {
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        String fieldName = keyNamePrefix + keyNumberGenerator.next();
        ByteIterator data = new RandomByteIterator( valueLengthGenerator.next(), random );
        values.put( fieldName, data );
        return values;
    }

    // TODO test / or remove entirely
    // Generate the next random keyNumber
    // NOTES:
    // Not sure how exactly this works
    // Seems to generate random numbers UNTIL it finds one matching a given
    // criteria (e.g. in range)
    // Seems inefficient and generally a bad approach
    public static DBRecordKey nextKey( Generator<Long> keyChooser, Generator<Long> keySequence )
            throws WorkloadException
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

    public static Generator<Long> buildFieldLengthGenerator( GeneratorBuilder generatorBuilder,
            Distribution distribution, Long lowerBound, Long upperBound, String histogramFilePath )
            throws WorkloadException
    {
        switch ( distribution )
        {
        case CONSTANT:
            return generatorBuilder.constantNumberGenerator( upperBound ).build();

        case UNIFORM:
            return generatorBuilder.uniformNumberGenerator( lowerBound, upperBound ).build();

        case ZIPFIAN:
            return generatorBuilder.zipfianGenerator( lowerBound, upperBound ).build();

        default:
            String errMsg = String.format( "Invalid Distribution [%s], use one of the following: %s, %s, %s, %s",
                    distribution, Distribution.CONSTANT, Distribution.UNIFORM, Distribution.ZIPFIAN );
            throw new WorkloadException( errMsg );
        }
    }
}
