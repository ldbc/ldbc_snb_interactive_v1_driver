package com.ldbc.workloads;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import com.ldbc.Client;
import com.ldbc.DB;
import com.ldbc.Workload;
import com.ldbc.WorkloadException;
import com.ldbc.generator.Generator;
import com.ldbc.generator.GeneratorBuilder;
import com.ldbc.generator.BoundedRangeExponentialNumberGenerator;
import com.ldbc.generator.MinMaxGeneratorWrapper;
import com.ldbc.util.Pair;
import com.ldbc.util.Utils;

// TODO there should not be a distinction between init() and initThread()
// TODO init() should just return an OperationGenerator
// TODO OperationGenerator should write to an OperationQueue
// TODO DriverThreads should read Operations from OperationQueue
// TODO DriverThreads should pass Operations to a DB implementation
// TODO DB implementation executes Operation against database
public class CoreWorkload extends Workload
{
    private final String KEY_NAME_PREFIX = "user";
    private final String FIELD_NAME_PREFIX = "field";

    private String TABLE;
    private boolean IS_ORDERED_INSERTS;

    // Insert/Update --- Constant/Uniform/Zipfian
    Generator<Integer> fieldValuelengthGenerator;
    // Insert --- Counter
    Generator<Long> loadInsertKeyGenerator;
    // Update/Read --- Exponential/Uniform/Scambled/Skewed/Hotspot
    Generator<Long> requestKeyGenerator;
    // Insert
    Generator<Set<String>> insertFieldSelectionGenerator;
    Generator<Set<String>> updateFieldSelectionGenerator; // Update
    Generator<Set<String>> readFieldSelectionGenerator; // Read
    Generator<Integer> scanLengthGenerator; // Read
                                            // Uniform/Zipfian
    MinMaxGeneratorWrapper<Long> transactionInsertKeyGenerator; // Request
                                                                // Counter
    Generator<String> operationGenerator; // Insert/Update/Read
                                          // Discrete

    // Initialization called once by client thread before operations start
    @Override
    public void init( Map<String, String> properties, GeneratorBuilder generatorBuilder ) throws WorkloadException
    {
        super.init( properties, generatorBuilder );
        TABLE = Utils.mapGetDefault( properties, CoreWorkloadProperties.TABLENAME,
                CoreWorkloadProperties.TABLENAME_DEFAULT );
        int fieldCount = Integer.parseInt( Utils.mapGetDefault( properties, CoreWorkloadProperties.FIELD_COUNT,
                CoreWorkloadProperties.FIELD_COUNT_DEFAULT ) );
        long recordCount = Long.parseLong( properties.get( Client.RECORD_COUNT ) );

        Distribution fieldLengthDistribution = Distribution.valueOf( Utils.mapGetDefault( properties,
                CoreWorkloadProperties.FIELD_LENGTH_DISTRIBUTION,
                CoreWorkloadProperties.FIELD_LENGTH_DISTRIBUTION_DEFAULT ).toUpperCase() );
        int maxFieldLength = Integer.parseInt( Utils.mapGetDefault( properties, CoreWorkloadProperties.FIELD_LENGTH,
                CoreWorkloadProperties.FIELD_LENGTH_DEFAULT ) );
        fieldValuelengthGenerator = WorkloadUtils.buildFieldLengthGenerator( generatorBuilder, fieldLengthDistribution,
                1, maxFieldLength );
        double readMixProportion = Double.parseDouble( Utils.mapGetDefault( properties,
                CoreWorkloadProperties.READ_PROPORTION, CoreWorkloadProperties.READ_PROPORTION_DEFAULT ) );
        double updateMixProportion = Double.parseDouble( Utils.mapGetDefault( properties,
                CoreWorkloadProperties.UPDATE_PROPORTION, CoreWorkloadProperties.UPDATE_PROPORTION_DEFAULT ) );
        double insertMixProportion = Double.parseDouble( Utils.mapGetDefault( properties,
                CoreWorkloadProperties.INSERT_PROPORTION, CoreWorkloadProperties.INSERT_PROPORTION_DEFAULT ) );
        double scanMixProportion = Double.parseDouble( Utils.mapGetDefault( properties,
                CoreWorkloadProperties.SCAN_PROPORTION, CoreWorkloadProperties.SCAN_PROPORTION_DEFAULT ) );
        double readModifyWriteMixProportion = Double.parseDouble( Utils.mapGetDefault( properties,
                CoreWorkloadProperties.READMODIFYWRITE_PROPORTION,
                CoreWorkloadProperties.READMODIFYWRITE_PROPORTION_DEFAULT ) );

        String requestDistribution = Utils.mapGetDefault( properties, CoreWorkloadProperties.REQUEST_DISTRIBUTION,
                CoreWorkloadProperties.REQUEST_DISTRIBUTION_DEFAULT );

        int maxScanlength = Integer.parseInt( Utils.mapGetDefault( properties, CoreWorkloadProperties.MAX_SCAN_LENGTH,
                CoreWorkloadProperties.MAX_SCAN_LENGTH_DEFAULT ) );
        String scanLengthDistribution = Utils.mapGetDefault( properties,
                CoreWorkloadProperties.SCAN_LENGTH_DISTRIBUTION,
                CoreWorkloadProperties.SCAN_LENGTH_DISTRIBUTION_DEFAULT );
        boolean isReadAllFields = Boolean.parseBoolean( Utils.mapGetDefault( properties,
                CoreWorkloadProperties.READ_ALL_FIELDS, CoreWorkloadProperties.READ_ALL_FIELDS_DEFAULT ) );
        boolean isWriteAllFields = Boolean.parseBoolean( Utils.mapGetDefault( properties,
                CoreWorkloadProperties.WRITE_ALL_FIELDS, CoreWorkloadProperties.WRITE_ALL_FIELDS_DEFAULT ) );

        if ( Utils.mapGetDefault( properties, CoreWorkloadProperties.INSERT_ORDER,
                CoreWorkloadProperties.INSERT_ORDER_DEFAULT ).equals( "hashed" ) )
        {
            IS_ORDERED_INSERTS = false;
        }
        else if ( requestDistribution.equals( "exponential" ) )
        {
            double percentile = Double.parseDouble( Utils.mapGetDefault( properties,
                    BoundedRangeExponentialNumberGenerator.EXPONENTIAL_PERCENTILE,
                    BoundedRangeExponentialNumberGenerator.EXPONENTIAL_PERCENTILE_DEFAULT ) );
            double frac = Double.parseDouble( Utils.mapGetDefault( properties,
                    BoundedRangeExponentialNumberGenerator.EXPONENTIAL_FRAC,
                    BoundedRangeExponentialNumberGenerator.EXPONENTIAL_FRAC_DEFAULT ) );
            requestKeyGenerator = generatorBuilder.exponentialGenerator( percentile, recordCount * frac ).build();
        }
        else
        {
            IS_ORDERED_INSERTS = true;
        }

        long insertStart = Long.parseLong( Utils.mapGetDefault( properties, Workload.INSERT_START,
                Workload.INSERT_START_DEFAULT ) );
        loadInsertKeyGenerator = generatorBuilder.counterGenerator( insertStart, 1l ).build();

        // proportion of transactions reads/update/insert/scan/read-modify-write
        ArrayList<Pair<Double, String>> operations = new ArrayList<Pair<Double, String>>();
        operations.add( new Pair<Double, String>( readMixProportion, "READ" ) );
        operations.add( new Pair<Double, String>( updateMixProportion, "UPDATE" ) );
        operations.add( new Pair<Double, String>( insertMixProportion, "INSERT" ) );
        operations.add( new Pair<Double, String>( scanMixProportion, "SCAN" ) );
        operations.add( new Pair<Double, String>( readModifyWriteMixProportion, "READMODIFYWRITE" ) );

        operationGenerator = generatorBuilder.discreteGenerator( operations ).build();

        transactionInsertKeyGenerator = generatorBuilder.counterGenerator( recordCount, 1l ).withMinMaxLast(
                recordCount, recordCount ).build();
        if ( requestDistribution.equals( "uniform" ) )
        {
            requestKeyGenerator = generatorBuilder.uniformNumberGenerator( 0l, recordCount - 1 ).build();
        }
        else if ( requestDistribution.equals( "zipfian" ) )
        {
            int operationCount = Integer.parseInt( properties.get( Client.OPERATION_COUNT ) );

            // 2 is fudge factor
            long expectednewkeys = new Double( operationCount * insertMixProportion * 2.0 ).longValue();

            requestKeyGenerator = generatorBuilder.scrambledZipfianGenerator( 0l, recordCount + expectednewkeys ).build();
        }
        else if ( requestDistribution.equals( "latest" ) )
        {
            requestKeyGenerator = generatorBuilder.skewedLatestGenerator( transactionInsertKeyGenerator ).build();
        }
        else if ( requestDistribution.equals( "hotspot" ) )
        {
            double hotSetFraction = Double.parseDouble( Utils.mapGetDefault( properties,
                    CoreWorkloadProperties.HOTSPOT_DATA_FRACTION, CoreWorkloadProperties.HOTSPOT_DATA_FRACTION_DEFAULT ) );
            double hotOperationFraction = Double.parseDouble( Utils.mapGetDefault( properties,
                    CoreWorkloadProperties.HOTSPOT_OPN_FRACTION, CoreWorkloadProperties.HOTSPOT_OPN_FRACTION_DEFAULT ) );
            requestKeyGenerator = generatorBuilder.dynamicRangeHotspotGenerator( 0, recordCount - 1, hotSetFraction,
                    hotOperationFraction ).build();
        }
        else
        {
            throw new WorkloadException( "Unknown request distribution \"" + requestDistribution + "\"" );
        }

        insertFieldSelectionGenerator = WorkloadUtils.buildFieldSelectionGenerator( generatorBuilder,
                FIELD_NAME_PREFIX, fieldCount, fieldCount );
        updateFieldSelectionGenerator = WorkloadUtils.buildFieldSelectionGenerator( generatorBuilder,
                FIELD_NAME_PREFIX, fieldCount, ( isWriteAllFields ) ? fieldCount : 1 );
        readFieldSelectionGenerator = WorkloadUtils.buildFieldSelectionGenerator( generatorBuilder, FIELD_NAME_PREFIX,
                fieldCount, ( isReadAllFields ) ? fieldCount : 1 );

        if ( scanLengthDistribution.equals( "uniform" ) )
        {
            scanLengthGenerator = generatorBuilder.uniformNumberGenerator( 1, maxScanlength ).build();
        }
        else if ( scanLengthDistribution.equals( "zipfian" ) )
        {
            scanLengthGenerator = generatorBuilder.zipfianNumberGenerator( 1, maxScanlength ).build();
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
        return WorkloadOperation.doInsert( db, loadInsertKeyGenerator, insertFieldSelectionGenerator,
                fieldValuelengthGenerator, IS_ORDERED_INSERTS, TABLE );
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

        if ( op.equals( "INSERT" ) )
        {
            return WorkloadOperation.doInsert( db, transactionInsertKeyGenerator, insertFieldSelectionGenerator,
                    fieldValuelengthGenerator, IS_ORDERED_INSERTS, TABLE );
        }
        else if ( op.equals( "READ" ) )
        {
            return WorkloadOperation.doRead( db, requestKeyGenerator, transactionInsertKeyGenerator,
                    readFieldSelectionGenerator, IS_ORDERED_INSERTS, TABLE );
        }
        else if ( op.equals( "UPDATE" ) )
        {
            return WorkloadOperation.doUpdate( db, requestKeyGenerator, transactionInsertKeyGenerator,
                    fieldValuelengthGenerator, readFieldSelectionGenerator, IS_ORDERED_INSERTS, TABLE );
        }
        else if ( op.equals( "SCAN" ) )
        {
            return WorkloadOperation.doScan( db, requestKeyGenerator, transactionInsertKeyGenerator,
                    scanLengthGenerator, readFieldSelectionGenerator, IS_ORDERED_INSERTS, TABLE );
        }
        else if ( op.equals( "READMODIFYWRITE" ) )
        {
            return WorkloadOperation.doReadModifyWrite( db, requestKeyGenerator, transactionInsertKeyGenerator,
                    readFieldSelectionGenerator, fieldValuelengthGenerator, IS_ORDERED_INSERTS, TABLE );
        }
        else
        {
            return false;
        }
    }
}
