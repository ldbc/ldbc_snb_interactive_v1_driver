package com.yahoo.ycsb.graph.workloads;

import java.util.Properties;

import com.google.common.collect.Range;

import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.graph.generator.CounterGenerator;
import com.yahoo.ycsb.graph.generator.DiscreteGenerator;
import com.yahoo.ycsb.graph.generator.Distribution;
import com.yahoo.ycsb.graph.generator.ExponentialGenerator;
import com.yahoo.ycsb.graph.generator.Generator;
import com.yahoo.ycsb.graph.generator.GeneratorFactory;
import com.yahoo.ycsb.graph.generator.HotspotIntegerGenerator;
import com.yahoo.ycsb.graph.generator.ScrambledZipfianGenerator;
import com.yahoo.ycsb.graph.generator.SkewedLatestGenerator;
import com.yahoo.ycsb.graph.generator.UniformIntegerGenerator;
import com.yahoo.ycsb.graph.generator.ZipfianGenerator;

public class CoreWorkload extends Workload
{
    public static String table;

    int fieldCount;
    boolean readAllFields;
    boolean writeAllFields;
    boolean orderedInserts;
    int recordCount;

    // TODO change class hierarchy to avoid unparameterized warnings?
    // value depends on "FIELD_LENGTH_" properties.
    Generator<Integer> fieldLengthGenerator;
    Generator<Integer> keySequence;
    Generator keyChooser;
    Generator<Integer> fieldChooser;
    Generator scanLength;
    CounterGenerator transactionInsertKeySequence;
    DiscreteGenerator operationChooser;

    /**
     * Initialize scenario. Called once, in main client thread, before
     * operations are started.
     */
    @Override
    public void init( Properties p ) throws WorkloadException
    {
        super.init( p );
        table = p.getProperty( CoreWorkloadProperties.TABLENAME, CoreWorkloadProperties.TABLENAME_DEFAULT );

        fieldCount = Integer.parseInt( p.getProperty( CoreWorkloadProperties.FIELD_COUNT,
                CoreWorkloadProperties.FIELD_COUNT_DEFAULT ) );
        recordCount = Integer.parseInt( p.getProperty( Client.RECORD_COUNT_PROPERTY ) );

        Distribution fieldLengthDistribution = Distribution.valueOf( p.getProperty(
                CoreWorkloadProperties.FIELD_LENGTH_DISTRIBUTION,
                CoreWorkloadProperties.FIELD_LENGTH_DISTRIBUTION_DEFAULT ).toUpperCase() );

        int fieldLength = Integer.parseInt( p.getProperty( CoreWorkloadProperties.FIELD_LENGTH,
                CoreWorkloadProperties.FIELD_LENGTH_DEFAULT ) );
        String fieldLengthHistogramFilePath = p.getProperty( CoreWorkloadProperties.FIELD_LENGTH_HISTOGRAM_FILE,
                CoreWorkloadProperties.FIELD_LENGTH_HISTOGRAM_FILE_DEFAULT );
        fieldLengthGenerator = WorkloadUtils.buildFieldLengthGenerator( fieldLengthDistribution,
                Range.closed( 1, fieldLength ), fieldLengthHistogramFilePath );

        double readProp = Double.parseDouble( p.getProperty( CoreWorkloadProperties.READ_PROPORTION,
                CoreWorkloadProperties.READ_PROPORTION_DEFAULT ) );
        double updateProp = Double.parseDouble( p.getProperty( CoreWorkloadProperties.UPDATE_PROPORTION,
                CoreWorkloadProperties.UPDATE_PROPORTION_DEFAULT ) );
        double insertProp = Double.parseDouble( p.getProperty( CoreWorkloadProperties.INSERT_PROPORTION,
                CoreWorkloadProperties.INSERT_PROPORTION_DEFAULT ) );
        double scanProp = Double.parseDouble( p.getProperty( CoreWorkloadProperties.SCAN_PROPORTION,
                CoreWorkloadProperties.SCAN_PROPORTION_DEFAULT ) );
        double readModifyWriteProp = Double.parseDouble( p.getProperty(
                CoreWorkloadProperties.READMODIFYWRITE_PROPORTION,
                CoreWorkloadProperties.READMODIFYWRITE_PROPORTION_DEFAULT ) );

        String requestdistrib = p.getProperty( CoreWorkloadProperties.REQUEST_DISTRIBUTION,
                CoreWorkloadProperties.REQUEST_DISTRIBUTION_DEFAULT );

        int maxscanlength = Integer.parseInt( p.getProperty( CoreWorkloadProperties.MAX_SCAN_LENGTH,
                CoreWorkloadProperties.MAX_SCAN_LENGTH_DEFAULT ) );
        String scanlengthdistrib = p.getProperty( CoreWorkloadProperties.SCAN_LENGTH_DISTRIBUTION,
                CoreWorkloadProperties.SCAN_LENGTH_DISTRIBUTION_DEFAULT );

        readAllFields = Boolean.parseBoolean( p.getProperty( CoreWorkloadProperties.READ_ALL_FIELDS,
                CoreWorkloadProperties.READ_ALL_FIELDS_DEFAULT ) );
        writeAllFields = Boolean.parseBoolean( p.getProperty( CoreWorkloadProperties.WRITE_ALL_FIELDS,
                CoreWorkloadProperties.WRITE_ALL_FIELDS_DEFAULT ) );

        if ( p.getProperty( CoreWorkloadProperties.INSERT_ORDER, CoreWorkloadProperties.INSERT_ORDER_DEFAULT ).compareTo(
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

        operationChooser = new GeneratorFactory().convertToDiscreteGenerator( readProp, "READ", updateProp, "UPDATE",
                insertProp, "INSERT", scanProp, "SCAN", readModifyWriteProp, "READMODIFYWRITE" );

        transactionInsertKeySequence = new CounterGenerator( recordCount );
        if ( requestdistrib.compareTo( "uniform" ) == 0 )
        {
            keyChooser = new UniformIntegerGenerator( 0, recordCount - 1 );
        }
        else if ( requestdistrib.compareTo( "zipfian" ) == 0 )
        {
            int opcount = Integer.parseInt( p.getProperty( Client.OPERATION_COUNT_PROPERTY ) );
            // 2 is fudge factor
            int expectednewkeys = (int) ( ( (double) opcount ) * insertProp * 2.0 );

            keyChooser = new ScrambledZipfianGenerator( recordCount + expectednewkeys );
        }
        else if ( requestdistrib.compareTo( "latest" ) == 0 )
        {
            keyChooser = new SkewedLatestGenerator( transactionInsertKeySequence );
        }
        else if ( requestdistrib.equals( "hotspot" ) )
        {
            double hotsetfraction = Double.parseDouble( p.getProperty( CoreWorkloadProperties.HOTSPOT_DATA_FRACTION,
                    CoreWorkloadProperties.HOTSPOT_DATA_FRACTION_DEFAULT ) );
            double hotopnfraction = Double.parseDouble( p.getProperty( CoreWorkloadProperties.HOTSPOT_OPN_FRACTION,
                    CoreWorkloadProperties.HOTSPOT_OPN_FRACTION_DEFAULT ) );
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

    /**
     * One insert operation. Called concurrently from multiple client threads,
     * must be thread safe. Avoid synchronized or threads will block waiting for
     * each other, and it will be difficult to reach target throughput. Function
     * should have no side effects other than DB operations.
     * 
     * @throws WorkloadException
     */
    @Override
    public boolean doInsert( DB db, Object threadstate )
    {
        try
        {
            return WorkloadOperation.doInsert( db, keySequence, orderedInserts, fieldCount, fieldLengthGenerator, table );
        }
        catch ( WorkloadException e )
        {
            // TODO should rethrow here, but Workload class needs to be modified
            System.out.println( "Error in doInsert : " + e.toString() );
            return false;
        }
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
        String op;
        try
        {
            op = (String) operationChooser.next()._2();
        }
        catch ( WorkloadException e )
        {
            // TODO throw exception (to do this Workload needs modifying)
            System.out.println( "Error in doTransaction 1: " + e.toString() );
            return false;
        }

        try
        {
            if ( op.equals( "READ" ) )
            {
                return WorkloadOperation.doRead( db, keyChooser, transactionInsertKeySequence, orderedInserts,
                        readAllFields, fieldChooser, table );
            }
            else if ( op.equals( "UPDATE" ) )
            {
                return WorkloadOperation.doUpdate( db, keyChooser, transactionInsertKeySequence, orderedInserts,
                        writeAllFields, fieldCount, fieldLengthGenerator, fieldChooser, table );
            }
            else if ( op.equals( "INSERT" ) )
            {
                return WorkloadOperation.doInsert( db, transactionInsertKeySequence, orderedInserts, fieldCount,
                        fieldLengthGenerator, table );
            }
            else if ( op.equals( "SCAN" ) )
            {
                return WorkloadOperation.doScan( db, keyChooser, transactionInsertKeySequence, orderedInserts,
                        scanLength, readAllFields, fieldChooser, table );
            }
            else if ( op.equals( "READMODIFYWRITE" ) )
            {
                return WorkloadOperation.doReadModifyWrite( db, keyChooser, transactionInsertKeySequence,
                        orderedInserts, readAllFields, writeAllFields, fieldChooser, table, fieldCount,
                        fieldLengthGenerator );
            }
            else
            {
                return false;
            }
        }
        catch ( WorkloadException e )
        {
            // TODO should rethrow here, but Workload class needs to be modified
            System.out.println( "Error in doTransaction 2: " + e.toString() );
            return false;
        }
    }
}
