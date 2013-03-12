package com.yahoo.ycsb.workloads;

import java.io.IOException;
import java.util.HashMap;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.RandomByteIterator;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.ConstantIntegerGenerator;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.ExponentialGenerator;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.HistogramGenerator;
import com.yahoo.ycsb.generator.IntegerGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import com.yahoo.ycsb.generator.ZipfianGenerator;

public class WorkloadUtils
{
    // TODO test
    // TODO rename
    // update all fields
    public static HashMap<String, ByteIterator> buildValues( int fieldCount, IntegerGenerator fieldLengthGenerator )
    {
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        for ( int i = 0; i < fieldCount; i++ )
        {
            String fieldkey = "field" + i;
            ByteIterator data = new RandomByteIterator( fieldLengthGenerator.nextInt() );
            values.put( fieldkey, data );
        }
        return values;
    }

    // TODO test
    // TODO rename
    // update a random field
    public static HashMap<String, ByteIterator> buildUpdate( Generator fieldchooser,
            IntegerGenerator fieldlengthgenerator )
    {
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        String fieldname = "field" + fieldchooser.nextString();
        ByteIterator data = new RandomByteIterator( fieldlengthgenerator.nextInt() );
        values.put( fieldname, data );
        return values;
    }

    // TODO test
    // TODO comments
    // TODO rename
    public static String buildKeyName( long keynum, boolean orderedInserts )
    {
        if ( !orderedInserts )
        {
            keynum = Utils.hash( keynum );
        }
        return "user" + keynum;
    }

    // TODO test
    // TODO comments
    // TODO rename
    public static int nextKeynum( IntegerGenerator keyChooser, CounterGenerator transactionInsertKeySequence )
    {
        int keyNum;
        if ( keyChooser instanceof ExponentialGenerator )
        {
            do
            {
                keyNum = transactionInsertKeySequence.lastInt() - keyChooser.nextInt();
            }
            while ( keyNum < 0 );
        }
        else
        {
            do
            {
                keyNum = keyChooser.nextInt();
            }
            while ( keyNum > transactionInsertKeySequence.lastInt() );
        }
        return keyNum;
    }

    // TODO test
    // TODO comments
    public static DiscreteGenerator buildOperationChooser( double readProportion, double updateProportion,
            double insertProportion, double scanProportion, double readModifyWriteProportion )
    {
        DiscreteGenerator operationChooser = new DiscreteGenerator();

        if ( readProportion > 0 )
        {
            operationChooser.addValue( readProportion, "READ" );
        }

        if ( updateProportion > 0 )
        {
            operationChooser.addValue( updateProportion, "UPDATE" );
        }

        if ( insertProportion > 0 )
        {
            operationChooser.addValue( insertProportion, "INSERT" );
        }

        if ( scanProportion > 0 )
        {
            operationChooser.addValue( scanProportion, "SCAN" );
        }

        if ( readModifyWriteProportion > 0 )
        {
            operationChooser.addValue( readModifyWriteProportion, "READMODIFYWRITE" );
        }

        return operationChooser;
    }

    // TODO test
    // TODO comments
    public static IntegerGenerator buildFieldLengthGenerator( String fieldLengthDistribution, int fieldLength,
            String fieldLengthHistogram ) throws WorkloadException
    {
        IntegerGenerator tempFieldlengthgenerator;
        if ( fieldLengthDistribution.compareTo( "constant" ) == 0 )
        {
            tempFieldlengthgenerator = new ConstantIntegerGenerator( fieldLength );
        }
        else if ( fieldLengthDistribution.compareTo( "uniform" ) == 0 )
        {
            tempFieldlengthgenerator = new UniformIntegerGenerator( 1, fieldLength );
        }
        else if ( fieldLengthDistribution.compareTo( "zipfian" ) == 0 )
        {
            tempFieldlengthgenerator = new ZipfianGenerator( 1, fieldLength );
        }
        else if ( fieldLengthDistribution.compareTo( "histogram" ) == 0 )
        {
            try
            {
                tempFieldlengthgenerator = new HistogramGenerator( fieldLengthHistogram );
            }
            catch ( IOException e )
            {
                throw new WorkloadException( "Couldn't read field length histogram file: " + fieldLengthHistogram, e );
            }
        }
        else
        {
            throw new WorkloadException( "Unknown field length distribution \"" + fieldLengthDistribution + "\"" );
        }
        return tempFieldlengthgenerator;
    }

}
