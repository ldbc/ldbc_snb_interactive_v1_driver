package com.yahoo.ycsb.workloads;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.RandomByteIterator;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.ConstantIntegerGenerator;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.ExponentialGenerator;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.HistogramGenerator;
import com.yahoo.ycsb.generator.HotspotIntegerGenerator;
import com.yahoo.ycsb.generator.IntegerGenerator;
import com.yahoo.ycsb.generator.ScrambledZipfianGenerator;
import com.yahoo.ycsb.generator.SkewedLatestGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import com.yahoo.ycsb.generator.ZipfianGenerator;
import com.yahoo.ycsb.measurements.Measurements;

public class GraphWorkload extends Workload
{
    public static String table;

    int fieldcount;
    boolean readallfields;
    boolean writeallfields;
    boolean orderedinserts;
    int recordcount;

    /**
     * value depends on "FIELD_LENGTH_" properties.
     */
    IntegerGenerator fieldlengthgenerator;
    IntegerGenerator keysequence;
    DiscreteGenerator operationchooser;
    IntegerGenerator keychooser;
    Generator fieldchooser;
    CounterGenerator transactioninsertkeysequence;
    IntegerGenerator scanlength;

    /**
     * Initialize scenario. Called once, in main client thread, before
     * operations are started.
     */
    @Override
    public void init( Properties p ) throws WorkloadException
    {
        super.init( p );
        table = p.getProperty( WorkloadProperties.TABLENAME, WorkloadProperties.TABLENAME_DEFAULT );

        fieldcount = Integer.parseInt( p.getProperty( WorkloadProperties.FIELD_COUNT,
                WorkloadProperties.FIELD_COUNT_DEFAULT ) );
        recordcount = Integer.parseInt( p.getProperty( Client.RECORD_COUNT_PROPERTY ) );

        String fieldlengthdistribution = p.getProperty( WorkloadProperties.FIELD_LENGTH_DISTRIBUTION,
                WorkloadProperties.FIELD_LENGTH_DISTRIBUTION_DEFAULT );
        int fieldlength = Integer.parseInt( p.getProperty( WorkloadProperties.FIELD_LENGTH,
                WorkloadProperties.FIELD_LENGTH_DEFAULT ) );
        String fieldlengthhistogram = p.getProperty( WorkloadProperties.FIELD_LENGTH_HISTOGRAM_FILE,
                WorkloadProperties.FIELD_LENGTH_HISTOGRAM_FILE_DEFAULT );
        fieldlengthgenerator = getFieldLengthGenerator( fieldlengthdistribution, fieldlength, fieldlengthhistogram );

        double readproportion = Double.parseDouble( p.getProperty( WorkloadProperties.READ_PROPORTION,
                WorkloadProperties.READ_PROPORTION_DEFAULT ) );
        double updateproportion = Double.parseDouble( p.getProperty( WorkloadProperties.UPDATE_PROPORTION,
                WorkloadProperties.UPDATE_PROPORTION_DEFAULT ) );
        double insertproportion = Double.parseDouble( p.getProperty( WorkloadProperties.INSERT_PROPORTION,
                WorkloadProperties.INSERT_PROPORTION_DEFAULT ) );
        double scanproportion = Double.parseDouble( p.getProperty( WorkloadProperties.SCAN_PROPORTION,
                WorkloadProperties.SCAN_PROPORTION_DEFAULT ) );
        double readmodifywriteproportion = Double.parseDouble( p.getProperty(
                WorkloadProperties.READMODIFYWRITE_PROPORTION, WorkloadProperties.READMODIFYWRITE_PROPORTION_DEFAULT ) );

        String requestdistrib = p.getProperty( WorkloadProperties.REQUEST_DISTRIBUTION,
                WorkloadProperties.REQUEST_DISTRIBUTION_DEFAULT );

        int maxscanlength = Integer.parseInt( p.getProperty( WorkloadProperties.MAX_SCAN_LENGTH,
                WorkloadProperties.MAX_SCAN_LENGTH_DEFAULT ) );
        String scanlengthdistrib = p.getProperty( WorkloadProperties.SCAN_LENGTH_DISTRIBUTION,
                WorkloadProperties.SCAN_LENGTH_DISTRIBUTION_DEFAULT );

        readallfields = Boolean.parseBoolean( p.getProperty( WorkloadProperties.READ_ALL_FIELDS,
                WorkloadProperties.READ_ALL_FIELDS_DEFAULT ) );
        writeallfields = Boolean.parseBoolean( p.getProperty( WorkloadProperties.WRITE_ALL_FIELDS,
                WorkloadProperties.WRITE_ALL_FIELDS_DEFAULT ) );

        if ( p.getProperty( WorkloadProperties.INSERT_ORDER, WorkloadProperties.INSERT_ORDER_DEFAULT ).compareTo(
                "hashed" ) == 0 )
        {
            orderedinserts = false;
        }
        else if ( requestdistrib.compareTo( "exponential" ) == 0 )
        {
            double percentile = Double.parseDouble( p.getProperty(
                    ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY,
                    ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT ) );
            double frac = Double.parseDouble( p.getProperty( ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY,
                    ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT ) );
            keychooser = new ExponentialGenerator( percentile, recordcount * frac );
        }
        else
        {
            orderedinserts = true;
        }

        int insertstart = Integer.parseInt( p.getProperty( Workload.INSERT_START_PROPERTY,
                Workload.INSERT_START_PROPERTY_DEFAULT ) );
        keysequence = new CounterGenerator( insertstart );

        operationchooser = getOperationChooser( readproportion, updateproportion, insertproportion, scanproportion,
                readmodifywriteproportion );

        transactioninsertkeysequence = new CounterGenerator( recordcount );
        if ( requestdistrib.compareTo( "uniform" ) == 0 )
        {
            keychooser = new UniformIntegerGenerator( 0, recordcount - 1 );
        }
        else if ( requestdistrib.compareTo( "zipfian" ) == 0 )
        {
            int opcount = Integer.parseInt( p.getProperty( Client.OPERATION_COUNT_PROPERTY ) );
            // 2 is fudge factor
            int expectednewkeys = (int) ( ( (double) opcount ) * insertproportion * 2.0 );

            keychooser = new ScrambledZipfianGenerator( recordcount + expectednewkeys );
        }
        else if ( requestdistrib.compareTo( "latest" ) == 0 )
        {
            keychooser = new SkewedLatestGenerator( transactioninsertkeysequence );
        }
        else if ( requestdistrib.equals( "hotspot" ) )
        {
            double hotsetfraction = Double.parseDouble( p.getProperty( WorkloadProperties.HOTSPOT_DATA_FRACTION,
                    WorkloadProperties.HOTSPOT_DATA_FRACTION_DEFAULT ) );
            double hotopnfraction = Double.parseDouble( p.getProperty( WorkloadProperties.HOTSPOT_OPN_FRACTION,
                    WorkloadProperties.HOTSPOT_OPN_FRACTION_DEFAULT ) );
            keychooser = new HotspotIntegerGenerator( 0, recordcount - 1, hotsetfraction, hotopnfraction );
        }
        else
        {
            throw new WorkloadException( "Unknown request distribution \"" + requestdistrib + "\"" );
        }

        fieldchooser = new UniformIntegerGenerator( 0, fieldcount - 1 );

        if ( scanlengthdistrib.compareTo( "uniform" ) == 0 )
        {
            scanlength = new UniformIntegerGenerator( 1, maxscanlength );
        }
        else if ( scanlengthdistrib.compareTo( "zipfian" ) == 0 )
        {
            scanlength = new ZipfianGenerator( 1, maxscanlength );
        }
        else
        {
            throw new WorkloadException( "Distribution \"" + scanlengthdistrib + "\" not allowed for scan length" );
        }
    }

    @Override
    public Object initThread( Properties p, int mythreadid, int threadcount ) throws WorkloadException
    {
        return super.initThread( p, mythreadid, threadcount );
    }

    /**
     * One insert operation. Called concurrently from multiple client threads,
     * must be thread safe. Avoid synchronized or threads will block waiting for
     * each other, and it will be difficult to reach target throughput. Function
     * should have no side effects other than DB operations.
     */
    @Override
    public boolean doInsert( DB db, Object threadstate )
    {
        int keynum = keysequence.nextInt();

        String dbkey = buildKeyName( keynum );

        HashMap<String, ByteIterator> values = buildValues();

        if ( db.insert( table, dbkey, values ) == 0 )
            return true;
        else
            return false;
    }

    /**
     * Do one transaction operation. Because it will be called concurrently from
     * multiple client threads, this function must be thread safe. However,
     * avoid synchronized, or the threads will block waiting for each other, and
     * it will be difficult to reach the target throughput. Ideally, this
     * function would have no side effects other than DB operations.
     */
    @Override
    public boolean doTransaction( DB db, Object threadstate )
    {
        String op = operationchooser.nextString();

        if ( op.compareTo( "READ" ) == 0 )
        {
            return doTransactionRead( db );
        }
        else if ( op.compareTo( "UPDATE" ) == 0 )
        {
            return doTransactionUpdate( db );
        }
        else if ( op.compareTo( "INSERT" ) == 0 )
        {
            return doTransactionInsert( db );
        }
        else if ( op.compareTo( "SCAN" ) == 0 )
        {
            return doTransactionScan( db );
        }
        else
        {
            return doTransactionReadModifyWrite( db );
        }
    }

    @Override
    public void cleanup() throws WorkloadException
    {
        super.cleanup();
    }

    @Override
    public void requestStop()
    {
        super.requestStop();
    }

    @Override
    public boolean isStopRequested()
    {
        return super.isStopRequested();
    }

    /**
     * Operations
     */

    public boolean doTransactionRead( DB db )
    {
        // choose a random key
        int keynum = nextKeynum();

        String keyname = buildKeyName( keynum );

        HashSet<String> fields = null;

        if ( !readallfields )
        {
            // read a random field
            String fieldname = "field" + fieldchooser.nextString();

            fields = new HashSet<String>();
            fields.add( fieldname );
        }

        if ( db.read( table, keyname, fields, new HashMap<String, ByteIterator>() ) == 0 )
            return true;
        else
            return false;
    }

    public boolean doTransactionReadModifyWrite( DB db )
    {
        // choose a random key
        int keynum = nextKeynum();

        String keyname = buildKeyName( keynum );

        HashSet<String> fields = null;

        if ( !readallfields )
        {
            // read a random field
            String fieldname = "field" + fieldchooser.nextString();

            fields = new HashSet<String>();
            fields.add( fieldname );
        }

        HashMap<String, ByteIterator> values;

        if ( writeallfields )
        {
            // new data for all the fields
            values = buildValues();
        }
        else
        {
            // update a random field
            values = buildUpdate();
        }

        // do the transaction

        long st = System.nanoTime();

        int result = db.read( table, keyname, fields, new HashMap<String, ByteIterator>() );

        result += db.update( table, keyname, values );

        long en = System.nanoTime();

        Measurements.getMeasurements().measure( "READ-MODIFY-WRITE", (int) ( ( en - st ) / 1000 ) );

        if ( result == 0 )
            return true;
        else
            return false;

    }

    public boolean doTransactionScan( DB db )
    {
        // choose a random key
        int keynum = nextKeynum();

        String startkeyname = buildKeyName( keynum );

        // choose a random scan length
        int len = scanlength.nextInt();

        HashSet<String> fields = null;

        if ( !readallfields )
        {
            // read a random field
            String fieldname = "field" + fieldchooser.nextString();

            fields = new HashSet<String>();
            fields.add( fieldname );
        }

        if ( db.scan( table, startkeyname, len, fields, new Vector<HashMap<String, ByteIterator>>() ) == 0 )
            return true;
        else
            return false;
    }

    public boolean doTransactionUpdate( DB db )
    {
        // choose a random key
        int keynum = nextKeynum();

        String keyname = buildKeyName( keynum );

        HashMap<String, ByteIterator> values;

        if ( writeallfields )
        {
            // new data for all the fields
            values = buildValues();
        }
        else
        {
            // update a random field
            values = buildUpdate();
        }

        if ( db.update( table, keyname, values ) == 0 )
            return true;
        else
            return false;
    }

    public boolean doTransactionInsert( DB db )
    {
        // choose the next key
        int keynum = transactioninsertkeysequence.nextInt();

        String dbkey = buildKeyName( keynum );

        HashMap<String, ByteIterator> values = buildValues();

        if ( db.insert( table, dbkey, values ) == 0 )
            return true;
        else
            return false;
    }

    /**
     * Helpers
     */

    private IntegerGenerator getFieldLengthGenerator( String fieldlengthdistribution, int fieldlength,
            String fieldlengthhistogram ) throws WorkloadException
    {
        IntegerGenerator fieldlengthgenerator;
        if ( fieldlengthdistribution.compareTo( "constant" ) == 0 )
        {
            fieldlengthgenerator = new ConstantIntegerGenerator( fieldlength );
        }
        else if ( fieldlengthdistribution.compareTo( "uniform" ) == 0 )
        {
            fieldlengthgenerator = new UniformIntegerGenerator( 1, fieldlength );
        }
        else if ( fieldlengthdistribution.compareTo( "zipfian" ) == 0 )
        {
            fieldlengthgenerator = new ZipfianGenerator( 1, fieldlength );
        }
        else if ( fieldlengthdistribution.compareTo( "histogram" ) == 0 )
        {
            try
            {
                fieldlengthgenerator = new HistogramGenerator( fieldlengthhistogram );
            }
            catch ( IOException e )
            {
                throw new WorkloadException( "Couldn't read field length histogram file: " + fieldlengthhistogram, e );
            }
        }
        else
        {
            throw new WorkloadException( "Unknown field length distribution \"" + fieldlengthdistribution + "\"" );
        }
        return fieldlengthgenerator;
    }

    private DiscreteGenerator getOperationChooser( double readproportion, double updateproportion,
            double insertproportion, double scanproportion, double readmodifywriteproportion )
    {
        DiscreteGenerator tempOperationchooser = new DiscreteGenerator();
        if ( readproportion > 0 )
        {
            tempOperationchooser.addValue( readproportion, "READ" );
        }

        if ( updateproportion > 0 )
        {
            tempOperationchooser.addValue( updateproportion, "UPDATE" );
        }

        if ( insertproportion > 0 )
        {
            tempOperationchooser.addValue( insertproportion, "INSERT" );
        }

        if ( scanproportion > 0 )
        {
            tempOperationchooser.addValue( scanproportion, "SCAN" );
        }

        if ( readmodifywriteproportion > 0 )
        {
            tempOperationchooser.addValue( readmodifywriteproportion, "READMODIFYWRITE" );
        }

        return tempOperationchooser;
    }

    private String buildKeyName( long keynum )
    {
        if ( !orderedinserts )
        {
            keynum = Utils.hash( keynum );
        }
        return "user" + keynum;
    }

    private HashMap<String, ByteIterator> buildValues()
    {
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        for ( int i = 0; i < fieldcount; i++ )
        {
            String fieldkey = "field" + i;
            ByteIterator data = new RandomByteIterator( fieldlengthgenerator.nextInt() );
            values.put( fieldkey, data );
        }
        return values;
    }

    private HashMap<String, ByteIterator> buildUpdate()
    {
        // update a random field
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        String fieldname = "field" + fieldchooser.nextString();
        ByteIterator data = new RandomByteIterator( fieldlengthgenerator.nextInt() );
        values.put( fieldname, data );
        return values;
    }

    private int nextKeynum()
    {
        int keynum;
        if ( keychooser instanceof ExponentialGenerator )
        {
            do
            {
                keynum = transactioninsertkeysequence.lastInt() - keychooser.nextInt();
            }
            while ( keynum < 0 );
        }
        else
        {
            do
            {
                keynum = keychooser.nextInt();
            }
            while ( keynum > transactioninsertkeysequence.lastInt() );
        }
        return keynum;
    }

}
