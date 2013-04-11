package com.yahoo.ycsb.workloads;

import java.util.Properties;

import com.google.common.collect.Range;

import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.ExponentialGenerator;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.GeneratorFactory;
import com.yahoo.ycsb.generator.Pair;

public class CoreWorkload extends Workload
{
    public static String table;

    int fieldCount;
    boolean readAllFields;
    boolean writeAllFields;
    boolean orderedInserts;
    int recordCount;

    Generator<Long> fieldLengthGenerator;
    Generator<Long> keySequence;
    Generator<Long> keyChooser;
    Generator<Long> fieldChooser;
    Generator<Long> scanLength;
    CounterGenerator transactionInsertKeySequence;
    Generator<? extends Object> operationGenerator;

    /**
     * Initialize scenario. Called once, in main client thread, before
     * operations are started.
     */
    @Override
    public void init( Properties p, GeneratorFactory generatorFactory ) throws WorkloadException
    {
        super.init( p, generatorFactory );
        table = p.getProperty( CoreWorkloadProperties.TABLENAME, CoreWorkloadProperties.TABLENAME_DEFAULT );

        fieldCount = Integer.parseInt( p.getProperty( CoreWorkloadProperties.FIELD_COUNT,
                CoreWorkloadProperties.FIELD_COUNT_DEFAULT ) );
        recordCount = Integer.parseInt( p.getProperty( Client.RECORD_COUNT ) );

        Distribution fieldLengthDistribution = Distribution.valueOf( p.getProperty(
                CoreWorkloadProperties.FIELD_LENGTH_DISTRIBUTION,
                CoreWorkloadProperties.FIELD_LENGTH_DISTRIBUTION_DEFAULT ).toUpperCase() );
        int fieldLength = Integer.parseInt( p.getProperty( CoreWorkloadProperties.FIELD_LENGTH,
                CoreWorkloadProperties.FIELD_LENGTH_DEFAULT ) );
        String fieldLengthHistogramFilePath = p.getProperty( CoreWorkloadProperties.FIELD_LENGTH_HISTOGRAM_FILE,
                CoreWorkloadProperties.FIELD_LENGTH_HISTOGRAM_FILE_DEFAULT );
        fieldLengthGenerator = WorkloadUtils.buildFieldLengthGenerator( fieldLengthDistribution,
                Range.closed( (long) 1, (long) fieldLength ), fieldLengthHistogramFilePath );

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

        int maxScanlength = Integer.parseInt( p.getProperty( CoreWorkloadProperties.MAX_SCAN_LENGTH,
                CoreWorkloadProperties.MAX_SCAN_LENGTH_DEFAULT ) );
        String scanLengthDistribution = p.getProperty( CoreWorkloadProperties.SCAN_LENGTH_DISTRIBUTION,
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
            double percentile = Double.parseDouble( p.getProperty( ExponentialGenerator.EXPONENTIAL_PERCENTILE,
                    ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT ) );
            double frac = Double.parseDouble( p.getProperty( ExponentialGenerator.EXPONENTIAL_FRAC,
                    ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT ) );
            keyChooser = generatorFactory.newExponentialGenerator( percentile, recordCount * frac );
        }
        else
        {
            orderedInserts = true;
        }

        int insertStart = Integer.parseInt( p.getProperty( Workload.INSERT_START, Workload.INSERT_START_DEFAULT ) );
        keySequence = generatorFactory.newCounterGenerator( insertStart );

        // proportion of transactions reads/update/insert/scan/read-modify-write
        Pair<Double, Object> readOperation = new Pair<Double, Object>( readProp, "READ" );
        Pair<Double, Object> updateOperation = new Pair<Double, Object>( updateProp, "UPDATE" );
        Pair<Double, Object> insertOperation = new Pair<Double, Object>( insertProp, "INSERT" );
        Pair<Double, Object> scanOperation = new Pair<Double, Object>( scanProp, "READ" );
        Pair<Double, Object> readModifyWriteOperation = new Pair<Double, Object>( readModifyWriteProp,
                "READMODIFYWRITE" );

        operationGenerator = generatorFactory.newDiscreteGenerator( readOperation, updateOperation, insertOperation,
                scanOperation, readModifyWriteOperation );

        transactionInsertKeySequence = generatorFactory.newCounterGenerator( recordCount );
        if ( requestdistrib.compareTo( "uniform" ) == 0 )
        {
            keyChooser = generatorFactory.newUniformIntegerGenerator( Range.closed( (long) 0, (long) ( recordCount - 1 ) ) );
        }
        else if ( requestdistrib.compareTo( "zipfian" ) == 0 )
        {
            int opcount = Integer.parseInt( p.getProperty( Client.OPERATION_COUNT ) );
            // 2 is fudge factor
            long expectednewkeys = (long) ( ( (double) opcount ) * insertProp * 2.0 );

            keyChooser = generatorFactory.newScrambledZipfianGenerator( Range.closed( 0l, recordCount + expectednewkeys ) );
        }
        else if ( requestdistrib.compareTo( "latest" ) == 0 )
        {
            keyChooser = generatorFactory.newSkewedLatestGenerator( transactionInsertKeySequence );
        }
        else if ( requestdistrib.equals( "hotspot" ) )
        {
            double hotsetfraction = Double.parseDouble( p.getProperty( CoreWorkloadProperties.HOTSPOT_DATA_FRACTION,
                    CoreWorkloadProperties.HOTSPOT_DATA_FRACTION_DEFAULT ) );
            double hotopnfraction = Double.parseDouble( p.getProperty( CoreWorkloadProperties.HOTSPOT_OPN_FRACTION,
                    CoreWorkloadProperties.HOTSPOT_OPN_FRACTION_DEFAULT ) );
            keyChooser = generatorFactory.newHotspotGenerator( 0, recordCount - 1, hotsetfraction, hotopnfraction );
        }
        else
        {
            throw new WorkloadException( "Unknown request distribution \"" + requestdistrib + "\"" );
        }

        fieldChooser = generatorFactory.newUniformIntegerGenerator( Range.closed( (long) 0, (long) ( fieldCount - 1 ) ) );

        if ( scanLengthDistribution.equals( "uniform" ) )
        {
            scanLength = generatorFactory.newUniformIntegerGenerator( Range.closed( (long) 1, (long) maxScanlength ) );
        }
        else if ( scanLengthDistribution.equals( "zipfian" ) )
        {
            scanLength = generatorFactory.newZipfianGenerator( Range.closed( (long) 1, (long) maxScanlength ) );
        }
        else
        {
            throw new WorkloadException( String.format( "Distributed [%s] not supported for scan length generator",
                    scanLengthDistribution ) );
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
    public boolean doInsert( DB db, Object threadstate ) throws WorkloadException
    {
        return WorkloadOperation.doInsert( db, keySequence, orderedInserts, fieldCount, fieldLengthGenerator, table );
    }

    /**
     * Do one transaction operation. Because it will be called concurrently from
     * multiple client threads, this function must be thread safe. However,
     * avoid synchronized, or the threads will block waiting for each other, and
     * it will be difficult to reach the target throughput. Ideally, this
     * function would have no side effects other than DB operations.
     * 
     * @throws WorkloadException
     */
    @Override
    public boolean doTransaction( DB db, Object threadstate ) throws WorkloadException
    {
        String op = (String) operationGenerator.next();

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
            return WorkloadOperation.doScan( db, keyChooser, transactionInsertKeySequence, orderedInserts, scanLength,
                    readAllFields, fieldChooser, table );
        }
        else if ( op.equals( "READMODIFYWRITE" ) )
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
