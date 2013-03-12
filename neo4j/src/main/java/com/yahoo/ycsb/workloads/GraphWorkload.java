package com.yahoo.ycsb.workloads;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.ExponentialGenerator;
import com.yahoo.ycsb.generator.Generator;
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

    int fieldCount;
    boolean readAllFields;
    boolean writeAllFields;
    boolean orderedInserts;
    int recordCount;

    // value depends on "FIELD_LENGTH_" properties.
    IntegerGenerator fieldLengthGenerator;
    IntegerGenerator keySequence;
    DiscreteGenerator operationChooser;
    IntegerGenerator keyChooser;
    Generator fieldChooser;
    CounterGenerator transactionInsertKeySequence;
    IntegerGenerator scanLength;

    /**
     * Initialize scenario. Called once, in main client thread, before
     * operations are started.
     */
    @Override
    public void init( Properties p ) throws WorkloadException
    {
        super.init( p );
        table = p.getProperty( WorkloadProperties.TABLENAME, WorkloadProperties.TABLENAME_DEFAULT );

        fieldCount = Integer.parseInt( p.getProperty( WorkloadProperties.FIELD_COUNT,
                WorkloadProperties.FIELD_COUNT_DEFAULT ) );
        recordCount = Integer.parseInt( p.getProperty( Client.RECORD_COUNT_PROPERTY ) );

        String fieldLengthDistribution = p.getProperty( WorkloadProperties.FIELD_LENGTH_DISTRIBUTION,
                WorkloadProperties.FIELD_LENGTH_DISTRIBUTION_DEFAULT );
        int fieldLength = Integer.parseInt( p.getProperty( WorkloadProperties.FIELD_LENGTH,
                WorkloadProperties.FIELD_LENGTH_DEFAULT ) );
        String fieldLengthHistogram = p.getProperty( WorkloadProperties.FIELD_LENGTH_HISTOGRAM_FILE,
                WorkloadProperties.FIELD_LENGTH_HISTOGRAM_FILE_DEFAULT );
        fieldLengthGenerator = WorkloadUtils.buildFieldLengthGenerator( fieldLengthDistribution, fieldLength,
                fieldLengthHistogram );

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

        readAllFields = Boolean.parseBoolean( p.getProperty( WorkloadProperties.READ_ALL_FIELDS,
                WorkloadProperties.READ_ALL_FIELDS_DEFAULT ) );
        writeAllFields = Boolean.parseBoolean( p.getProperty( WorkloadProperties.WRITE_ALL_FIELDS,
                WorkloadProperties.WRITE_ALL_FIELDS_DEFAULT ) );

        if ( p.getProperty( WorkloadProperties.INSERT_ORDER, WorkloadProperties.INSERT_ORDER_DEFAULT ).compareTo(
                "hashed" ) == 0 )
        {
            orderedInserts = false;
        }
        else if ( requestdistrib.compareTo( "exponential" ) == 0 )
        {
            double percentile = Double.parseDouble( p.getProperty(
                    ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY,
                    ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT ) );
            double frac = Double.parseDouble( p.getProperty( ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY,
                    ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT ) );
            keyChooser = new ExponentialGenerator( percentile, recordCount * frac );
        }
        else
        {
            orderedInserts = true;
        }

        int insertstart = Integer.parseInt( p.getProperty( Workload.INSERT_START_PROPERTY,
                Workload.INSERT_START_PROPERTY_DEFAULT ) );
        keySequence = new CounterGenerator( insertstart );

        operationChooser = WorkloadUtils.buildOperationChooser( readproportion, updateproportion, insertproportion,
                scanproportion, readmodifywriteproportion );

        transactionInsertKeySequence = new CounterGenerator( recordCount );
        if ( requestdistrib.compareTo( "uniform" ) == 0 )
        {
            keyChooser = new UniformIntegerGenerator( 0, recordCount - 1 );
        }
        else if ( requestdistrib.compareTo( "zipfian" ) == 0 )
        {
            int opcount = Integer.parseInt( p.getProperty( Client.OPERATION_COUNT_PROPERTY ) );
            // 2 is fudge factor
            int expectednewkeys = (int) ( ( (double) opcount ) * insertproportion * 2.0 );

            keyChooser = new ScrambledZipfianGenerator( recordCount + expectednewkeys );
        }
        else if ( requestdistrib.compareTo( "latest" ) == 0 )
        {
            keyChooser = new SkewedLatestGenerator( transactionInsertKeySequence );
        }
        else if ( requestdistrib.equals( "hotspot" ) )
        {
            double hotsetfraction = Double.parseDouble( p.getProperty( WorkloadProperties.HOTSPOT_DATA_FRACTION,
                    WorkloadProperties.HOTSPOT_DATA_FRACTION_DEFAULT ) );
            double hotopnfraction = Double.parseDouble( p.getProperty( WorkloadProperties.HOTSPOT_OPN_FRACTION,
                    WorkloadProperties.HOTSPOT_OPN_FRACTION_DEFAULT ) );
            keyChooser = new HotspotIntegerGenerator( 0, recordCount - 1, hotsetfraction, hotopnfraction );
        }
        else
        {
            throw new WorkloadException( "Unknown request distribution \"" + requestdistrib + "\"" );
        }

        fieldChooser = new UniformIntegerGenerator( 0, fieldCount - 1 );

        if ( scanlengthdistrib.compareTo( "uniform" ) == 0 )
        {
            scanLength = new UniformIntegerGenerator( 1, maxscanlength );
        }
        else if ( scanlengthdistrib.compareTo( "zipfian" ) == 0 )
        {
            scanLength = new ZipfianGenerator( 1, maxscanlength );
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
        int keynum = keySequence.nextInt();

        String dbkey = WorkloadUtils.buildKeyName( keynum, orderedInserts );

        HashMap<String, ByteIterator> values = WorkloadUtils.buildValues( fieldCount, fieldLengthGenerator );

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
     * 
     * @throws DBException
     */
    @Override
    public boolean doTransaction( DB db, Object threadstate )
    {
        String op = operationChooser.nextString();

        if ( op.compareTo( "READ" ) == 0 )
        {
            return WorkloadOperation.doRead( db, keyChooser, transactionInsertKeySequence, orderedInserts,
                    readAllFields, fieldChooser, table );
        }
        else if ( op.compareTo( "UPDATE" ) == 0 )
        {
            return WorkloadOperation.doUpdate( db, keyChooser, transactionInsertKeySequence, orderedInserts,
                    writeAllFields, fieldCount, fieldLengthGenerator, fieldChooser, table );
        }
        else if ( op.compareTo( "INSERT" ) == 0 )
        {
            return WorkloadOperation.doInsert( db, transactionInsertKeySequence, orderedInserts, fieldCount,
                    fieldLengthGenerator, table );
        }
        else if ( op.compareTo( "SCAN" ) == 0 )
        {
            return WorkloadOperation.doScan( db, keyChooser, transactionInsertKeySequence, orderedInserts, scanLength,
                    readAllFields, fieldChooser, table );
        }
        else if ( op.compareTo( "READMODIFYWRITE" ) == 0 )
        {
            return WorkloadOperation.doReadModifyWrite( db, keyChooser, transactionInsertKeySequence, orderedInserts,
                    readAllFields, writeAllFields, fieldChooser, table, fieldCount, fieldLengthGenerator );

        }
        else
        {
            return false;
        }
    }
}
