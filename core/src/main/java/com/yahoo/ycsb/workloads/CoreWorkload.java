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

    long fieldCount;
    boolean readAllFields;
    boolean writeAllFields;

    boolean orderedInserts;
    long recordCount;

    Generator<Long> fieldLengthGenerator; // Insert/Update
                                          // Constant/Uniform/Zipfian
    Generator<Long> insertKeyGenerator; // Insert
                                        // Counter
    Generator<Long> requestKeyGenerator; // Update/Read
                                         // Exponential/Uniform/Scambled/Skewed/Hotspot
    Generator<Long> fieldChooserGenerator; // Update/Read
                                           // Uniform
    Generator<Long> scanLengthGenerator; // Read
                                         // Uniform/Zipfian
    MinMaxGeneratorWrapper<Long> transactionInsertKeySequenceGenerator; // Request
                                                                        // Counter
    Generator<String> operationGenerator; // Insert/Update/Read
                                          // Discrete

    // Initialization called once by client thread before operations start
    @Override
    public void init( Map<String, String> properties, GeneratorBuilder generatorBuilder ) throws WorkloadException
    {
        super.init( properties, generatorBuilder );
        table = Utils.mapGetDefault( properties, CoreWorkloadProperties.TABLENAME,
                CoreWorkloadProperties.TABLENAME_DEFAULT );
        fieldCount = Long.parseLong( Utils.mapGetDefault( properties, CoreWorkloadProperties.FIELD_COUNT,
                CoreWorkloadProperties.FIELD_COUNT_DEFAULT ) );
        recordCount = Long.parseLong( properties.get( Client.RECORD_COUNT ) );

        Distribution fieldLengthDistribution = Distribution.valueOf( Utils.mapGetDefault( properties,
                CoreWorkloadProperties.FIELD_LENGTH_DISTRIBUTION,
                CoreWorkloadProperties.FIELD_LENGTH_DISTRIBUTION_DEFAULT ).toUpperCase() );
        long fieldLength = Long.parseLong( Utils.mapGetDefault( properties, CoreWorkloadProperties.FIELD_LENGTH,
                CoreWorkloadProperties.FIELD_LENGTH_DEFAULT ) );
        String fieldLengthHistogramFilePath = Utils.mapGetDefault( properties,
                CoreWorkloadProperties.FIELD_LENGTH_HISTOGRAM_FILE,
                CoreWorkloadProperties.FIELD_LENGTH_HISTOGRAM_FILE_DEFAULT );
        fieldLengthGenerator = WorkloadUtils.buildFieldLengthGenerator( generatorBuilder, fieldLengthDistribution, 1l,
                fieldLength, fieldLengthHistogramFilePath );
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

        String requestDistribution = Utils.mapGetDefault( properties, CoreWorkloadProperties.REQUEST_DISTRIBUTION,
                CoreWorkloadProperties.REQUEST_DISTRIBUTION_DEFAULT );

        long maxScanlength = Long.parseLong( Utils.mapGetDefault( properties, CoreWorkloadProperties.MAX_SCAN_LENGTH,
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
        else if ( requestDistribution.equals( "exponential" ) )
        {
            double percentile = Double.parseDouble( Utils.mapGetDefault( properties,
                    ExponentialGenerator.EXPONENTIAL_PERCENTILE, ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT ) );
            double frac = Double.parseDouble( Utils.mapGetDefault( properties, ExponentialGenerator.EXPONENTIAL_FRAC,
                    ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT ) );
            requestKeyGenerator = generatorBuilder.exponentialGenerator( percentile, recordCount * frac ).build();
        }
        else
        {
            orderedInserts = true;
        }

        long insertStart = Long.parseLong( Utils.mapGetDefault( properties, Workload.INSERT_START,
                Workload.INSERT_START_DEFAULT ) );
        insertKeyGenerator = generatorBuilder.counterGenerator( insertStart, 1l ).build();

        // proportion of transactions reads/update/insert/scan/read-modify-write
        ArrayList<Pair<Double, String>> operations = new ArrayList<Pair<Double, String>>();
        operations.add( new Pair<Double, String>( readProp, "READ" ) );
        operations.add( new Pair<Double, String>( updateProp, "UPDATE" ) );
        operations.add( new Pair<Double, String>( insertProp, "INSERT" ) );
        operations.add( new Pair<Double, String>( scanProp, "SCAN" ) );
        operations.add( new Pair<Double, String>( readModifyWriteProp, "READMODIFYWRITE" ) );

        operationGenerator = generatorBuilder.discreteGenerator( operations ).build();

        transactionInsertKeySequenceGenerator = generatorBuilder.counterGenerator( recordCount, 1l ).withMinMaxLast(
                recordCount, recordCount ).build();
        if ( requestDistribution.equals( "uniform" ) )
        {
            requestKeyGenerator = generatorBuilder.uniformNumberGenerator( 0l, recordCount - 1 ).build();
        }
        else if ( requestDistribution.equals( "zipfian" ) )
        {
            int operationCount = Integer.parseInt( properties.get( Client.OPERATION_COUNT ) );

            // 2 is fudge factor
            long expectednewkeys = new Double( operationCount * insertProp * 2.0 ).longValue();

            requestKeyGenerator = generatorBuilder.scrambledZipfianGenerator( 0l, recordCount + expectednewkeys ).build();
        }
        else if ( requestDistribution.equals( "latest" ) )
        {
            requestKeyGenerator = generatorBuilder.skewedLatestGenerator( transactionInsertKeySequenceGenerator ).build();
        }
        else if ( requestDistribution.equals( "hotspot" ) )
        {
            double hotSetFraction = Double.parseDouble( Utils.mapGetDefault( properties,
                    CoreWorkloadProperties.HOTSPOT_DATA_FRACTION, CoreWorkloadProperties.HOTSPOT_DATA_FRACTION_DEFAULT ) );
            double hotOperationFraction = Double.parseDouble( Utils.mapGetDefault( properties,
                    CoreWorkloadProperties.HOTSPOT_OPN_FRACTION, CoreWorkloadProperties.HOTSPOT_OPN_FRACTION_DEFAULT ) );
            requestKeyGenerator = generatorBuilder.hotspotGenerator( 0, recordCount - 1, hotSetFraction,
                    hotOperationFraction ).build();
        }
        else
        {
            throw new WorkloadException( "Unknown request distribution \"" + requestDistribution + "\"" );
        }

        fieldChooserGenerator = generatorBuilder.uniformNumberGenerator( 0l, fieldCount - 1 ).build();

        if ( scanLengthDistribution.equals( "uniform" ) )
        {
            scanLengthGenerator = generatorBuilder.uniformNumberGenerator( 1l, maxScanlength ).build();
        }
        else if ( scanLengthDistribution.equals( "zipfian" ) )
        {
            scanLengthGenerator = generatorBuilder.zipfianGenerator( (long) 1, (long) maxScanlength ).build();
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
        return WorkloadOperation.doInsert( db, insertKeyGenerator, orderedInserts, fieldCount, fieldLengthGenerator,
                table );
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
            return WorkloadOperation.doRead( db, requestKeyGenerator, transactionInsertKeySequenceGenerator,
                    orderedInserts, readAllFields, fieldChooserGenerator, table );
        }
        else if ( op.equals( "UPDATE" ) )
        {
            return WorkloadOperation.doUpdate( db, requestKeyGenerator, transactionInsertKeySequenceGenerator,
                    orderedInserts, writeAllFields, fieldCount, fieldLengthGenerator, fieldChooserGenerator, table );
        }
        else if ( op.equals( "INSERT" ) )
        {
            return WorkloadOperation.doInsert( db, transactionInsertKeySequenceGenerator, orderedInserts, fieldCount,
                    fieldLengthGenerator, table );
        }
        else if ( op.equals( "SCAN" ) )
        {
            return WorkloadOperation.doScan( db, requestKeyGenerator, transactionInsertKeySequenceGenerator,
                    orderedInserts, scanLengthGenerator, readAllFields, fieldChooserGenerator, table );
        }
        else if ( op.equals( "READMODIFYWRITE" ) )
        {
            return WorkloadOperation.doReadModifyWrite( db, requestKeyGenerator, transactionInsertKeySequenceGenerator,
                    orderedInserts, readAllFields, writeAllFields, fieldChooserGenerator, table, fieldCount,
                    fieldLengthGenerator );
        }
        else
        {
            return false;
        }
    }
}
