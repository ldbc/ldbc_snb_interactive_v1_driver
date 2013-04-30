package com.yahoo.ycsb.workloads;

import java.util.ArrayList;
import java.util.Map;

import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.GeneratorBuilder;
import com.yahoo.ycsb.generator.MinMaxGeneratorWrapper;
import com.yahoo.ycsb.generator.ycsb.ExponentialGenerator;
import com.yahoo.ycsb.util.Pair;
import com.yahoo.ycsb.util.Utils;

public class CoreWorkload extends Workload
{
    public static String table;

    int fieldCount;
    boolean readAllFields;
    boolean writeAllFields;
    boolean orderedInserts;
    int recordCount;

    // TODO make composite generator with dynamic upper limit
    Generator<Long> fieldLengthGenerator;
    Generator<Long> keySequence;
    Generator<Long> keyChooser;
    Generator<Long> fieldChooser;
    Generator<Long> scanLength;
    MinMaxGeneratorWrapper<Long> transactionInsertKeySequence;
    Generator<? extends Object> operationGenerator;

    /**
     * Initialize scenario. Called once, in main client thread, before
     * operations are started.
     */
    @Override
    public void init( Map<String, String> properties, GeneratorBuilder generatorBuilder ) throws WorkloadException
    {
        super.init( properties, generatorBuilder );
        table = Utils.mapGetDefault( properties, CoreWorkloadProperties.TABLENAME,
                CoreWorkloadProperties.TABLENAME_DEFAULT );
        fieldCount = Integer.parseInt( Utils.mapGetDefault( properties, CoreWorkloadProperties.FIELD_COUNT,
                CoreWorkloadProperties.FIELD_COUNT_DEFAULT ) );
        recordCount = Integer.parseInt( properties.get( Client.RECORD_COUNT ) );

        Distribution fieldLengthDistribution = Distribution.valueOf( Utils.mapGetDefault( properties,
                CoreWorkloadProperties.FIELD_LENGTH_DISTRIBUTION,
                CoreWorkloadProperties.FIELD_LENGTH_DISTRIBUTION_DEFAULT ).toUpperCase() );
        int fieldLength = Integer.parseInt( Utils.mapGetDefault( properties, CoreWorkloadProperties.FIELD_LENGTH,
                CoreWorkloadProperties.FIELD_LENGTH_DEFAULT ) );
        String fieldLengthHistogramFilePath = Utils.mapGetDefault( properties,
                CoreWorkloadProperties.FIELD_LENGTH_HISTOGRAM_FILE,
                CoreWorkloadProperties.FIELD_LENGTH_HISTOGRAM_FILE_DEFAULT );
        fieldLengthGenerator = WorkloadUtils.buildFieldLengthGenerator( fieldLengthDistribution, (long) 1,
                (long) fieldLength, fieldLengthHistogramFilePath );
        double readProp = Double.parseDouble( Utils.mapGetDefault( properties, CoreWorkloadProperties.READ_PROPORTION,
                CoreWorkloadProperties.READ_PROPORTION_DEFAULT ) );
        double updateProp = Double.parseDouble( Utils.mapGetDefault( properties,
                CoreWorkloadProperties.UPDATE_PROPORTION, CoreWorkloadProperties.UPDATE_PROPORTION_DEFAULT ) );
        double insertProp = Double.parseDouble( Utils.mapGetDefault( properties,
                CoreWorkloadProperties.INSERT_PROPORTION, CoreWorkloadProperties.INSERT_PROPORTION_DEFAULT ) );
        double scanProp = Double.parseDouble( Utils.mapGetDefault( properties, CoreWorkloadProperties.SCAN_PROPORTION,
                CoreWorkloadProperties.SCAN_PROPORTION_DEFAULT ) );
        double readModifyWriteProp = Double.parseDouble( Utils.mapGetDefault( properties,
                CoreWorkloadProperties.READMODIFYWRITE_PROPORTION,
                CoreWorkloadProperties.READMODIFYWRITE_PROPORTION_DEFAULT ) );

        String requestdistrib = Utils.mapGetDefault( properties, CoreWorkloadProperties.REQUEST_DISTRIBUTION,
                CoreWorkloadProperties.REQUEST_DISTRIBUTION_DEFAULT );

        int maxScanlength = Integer.parseInt( Utils.mapGetDefault( properties, CoreWorkloadProperties.MAX_SCAN_LENGTH,
                CoreWorkloadProperties.MAX_SCAN_LENGTH_DEFAULT ) );
        String scanLengthDistribution = Utils.mapGetDefault( properties,
                CoreWorkloadProperties.SCAN_LENGTH_DISTRIBUTION,
                CoreWorkloadProperties.SCAN_LENGTH_DISTRIBUTION_DEFAULT );
        readAllFields = Boolean.parseBoolean( Utils.mapGetDefault( properties, CoreWorkloadProperties.READ_ALL_FIELDS,
                CoreWorkloadProperties.READ_ALL_FIELDS_DEFAULT ) );
        writeAllFields = Boolean.parseBoolean( Utils.mapGetDefault( properties,
                CoreWorkloadProperties.WRITE_ALL_FIELDS, CoreWorkloadProperties.WRITE_ALL_FIELDS_DEFAULT ) );

        if ( Utils.mapGetDefault( properties, CoreWorkloadProperties.INSERT_ORDER,
                CoreWorkloadProperties.INSERT_ORDER_DEFAULT ).equals( "hashed" ) )
        {
            orderedInserts = false;
        }
        else if ( requestdistrib.compareTo( "exponential" ) == 0 )
        {
            double percentile = Double.parseDouble( Utils.mapGetDefault( properties,
                    ExponentialGenerator.EXPONENTIAL_PERCENTILE, ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT ) );
            double frac = Double.parseDouble( Utils.mapGetDefault( properties, ExponentialGenerator.EXPONENTIAL_FRAC,
                    ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT ) );
            keyChooser = generatorBuilder.newExponentialGenerator( percentile, recordCount * frac ).build();
        }
        else
        {
            orderedInserts = true;
        }

        long insertStart = Long.parseLong( Utils.mapGetDefault( properties, Workload.INSERT_START,
                Workload.INSERT_START_DEFAULT ) );
        keySequence = generatorBuilder.newCounterGenerator( insertStart, 1l ).build();

        // proportion of transactions reads/update/insert/scan/read-modify-write
        ArrayList<Pair<Double, Object>> operations = new ArrayList<Pair<Double, Object>>();
        operations.add( new Pair<Double, Object>( readProp, "READ" ) );
        operations.add( new Pair<Double, Object>( updateProp, "UPDATE" ) );
        operations.add( new Pair<Double, Object>( insertProp, "INSERT" ) );
        operations.add( new Pair<Double, Object>( scanProp, "SCAN" ) );
        operations.add( new Pair<Double, Object>( readModifyWriteProp, "READMODIFYWRITE" ) );

        operationGenerator = generatorBuilder.newDiscreteGenerator( operations ).build();

        // TODO should not cast
        transactionInsertKeySequence = (MinMaxGeneratorWrapper<Long>) generatorBuilder.newCounterGenerator(
                (long) recordCount, 1l ).withMinMaxLast( (long) recordCount, (long) recordCount ).build();
        if ( requestdistrib.compareTo( "uniform" ) == 0 )
        {
            keyChooser = generatorBuilder.newUniformNumberGenerator( (long) 0, (long) ( recordCount - 1 ) ).build();
        }
        else if ( requestdistrib.compareTo( "zipfian" ) == 0 )
        {
            int opcount = Integer.parseInt( properties.get( Client.OPERATION_COUNT ) );

            // 2 is fudge factor
            long expectednewkeys = (long) ( ( (double) opcount ) * insertProp * 2.0 );

            keyChooser = generatorBuilder.newScrambledZipfianGenerator( 0l, recordCount + expectednewkeys ).build();
        }
        else if ( requestdistrib.compareTo( "latest" ) == 0 )
        {
            keyChooser = generatorBuilder.newSkewedLatestGenerator( transactionInsertKeySequence ).build();
        }
        else if ( requestdistrib.equals( "hotspot" ) )
        {
            double hotsetfraction = Double.parseDouble( Utils.mapGetDefault( properties,
                    CoreWorkloadProperties.HOTSPOT_DATA_FRACTION, CoreWorkloadProperties.HOTSPOT_DATA_FRACTION_DEFAULT ) );
            double hotopnfraction = Double.parseDouble( Utils.mapGetDefault( properties,
                    CoreWorkloadProperties.HOTSPOT_OPN_FRACTION, CoreWorkloadProperties.HOTSPOT_OPN_FRACTION_DEFAULT ) );
            keyChooser = generatorBuilder.newHotspotGenerator( 0, recordCount - 1, hotsetfraction, hotopnfraction ).build();
        }
        else
        {
            throw new WorkloadException( "Unknown request distribution \"" + requestdistrib + "\"" );
        }

        fieldChooser = generatorBuilder.newUniformNumberGenerator( (long) 0, (long) ( fieldCount - 1 ) ).build();

        if ( scanLengthDistribution.equals( "uniform" ) )
        {
            scanLength = generatorBuilder.newUniformNumberGenerator( (long) 1, (long) maxScanlength ).build();
        }
        else if ( scanLengthDistribution.equals( "zipfian" ) )
        {
            scanLength = generatorBuilder.newZipfianGenerator( (long) 1, (long) maxScanlength ).build();
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
