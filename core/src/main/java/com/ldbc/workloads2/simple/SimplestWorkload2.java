package com.ldbc.workloads2.simple;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import com.ldbc.Client;
import com.ldbc.DB;
import com.ldbc.DBException;
import com.ldbc.generator.Generator;
import com.ldbc.generator.GeneratorBuilder;
import com.ldbc.generator.MinMaxGeneratorWrapper;
import com.ldbc.util.Pair;
import com.ldbc.util.MapUtils;
import com.ldbc.workloads.Workload;
import com.ldbc.workloads.WorkloadException;
import com.ldbc.workloads.ycsb.WorkloadUtils;

public class SimplestWorkload2 extends Workload
{
    final String KEY_NAME_PREFIX = "user";
    final String FIELD_NAME_PREFIX = "field";
    final String TABLE = "usertable";
    final int NUMBER_OF_FIELDS_IN_RECORD = 10;

    boolean IS_ORDERED_INSERTS;

    Generator<Long> loadInsertKeyGenerator;
    Generator<Integer> fieldValuelengthGenerator;
    Generator<Long> requestKeyGenerator;
    Generator<Set<String>> insertFieldSelectionGenerator;
    Generator<Set<String>> updateFieldSelectionGenerator;
    Generator<Set<String>> readFieldSelectionGenerator;
    Generator<Integer> scanLengthGenerator;
    MinMaxGeneratorWrapper<Long> transactionInsertKeyGenerator;
    Generator<String> operationGenerator;

    @Override
    public void init( Map<String, String> properties, GeneratorBuilder generatorBuilder ) throws WorkloadException
    {
        super.init( properties, generatorBuilder );
        long recordCount = Long.parseLong( properties.get( Client.RECORD_COUNT ) );

        // read one field (false) or all fields (true) of a record
        boolean isReadAllFields = false;
        // write one field (false) or all fields (true) of a record
        boolean isWriteAllFields = false;

        insertFieldSelectionGenerator = WorkloadUtils.buildFieldSelectionGenerator( generatorBuilder,
                FIELD_NAME_PREFIX, NUMBER_OF_FIELDS_IN_RECORD, NUMBER_OF_FIELDS_IN_RECORD );
        updateFieldSelectionGenerator = WorkloadUtils.buildFieldSelectionGenerator( generatorBuilder,
                FIELD_NAME_PREFIX, NUMBER_OF_FIELDS_IN_RECORD, ( isWriteAllFields ) ? NUMBER_OF_FIELDS_IN_RECORD : 1 );
        readFieldSelectionGenerator = WorkloadUtils.buildFieldSelectionGenerator( generatorBuilder, FIELD_NAME_PREFIX,
                NUMBER_OF_FIELDS_IN_RECORD, ( isReadAllFields ) ? NUMBER_OF_FIELDS_IN_RECORD : 1 );

        // field value length in bytes
        fieldValuelengthGenerator = generatorBuilder.uniformNumberGenerator( 1, 100 ).build();

        // proportion of transactions reads/update/insert/scan/read-modify-write
        ArrayList<Pair<Double, String>> operations = new ArrayList<Pair<Double, String>>();
        operations.add( Pair.create( 0.95, "READ" ) );
        operations.add( Pair.create( 0.05, "UPDATE" ) );
        operations.add( Pair.create( 0.00, "INSERT" ) );
        operations.add( Pair.create( 0.00, "SCAN" ) );
        operations.add( Pair.create( 0.00, "READMODIFYWRITE" ) );

        // max scan length (number of records)
        int maxScanlength = 1000;
        scanLengthGenerator = generatorBuilder.uniformNumberGenerator( 1, maxScanlength ).build();

        // order to insert records: "ordered" (true), "hashed" (false)
        IS_ORDERED_INSERTS = false;

        /* 
         * INSERT_START
         * Specifies which record ID each client starts from - enables load phase to proceed from 
         * multiple clients on different machines.
         * 
         * INSERT_COUNT
         * Interpreted by Client, tells each client instance of the how many inserts to do.
         *  
         * E.g. to load 1,000,000 records from 2 machines: 
         * client 1 --> insertStart=0
         *          --> insertCount=500,000
         * client 2 --> insertStart=50,000
         *          --> insertCount=500,000
        */
        long insertStart = Long.parseLong( MapUtils.mapGetDefault( properties, Workload.INSERT_START,
                Workload.INSERT_START_DEFAULT ) );
        loadInsertKeyGenerator = generatorBuilder.counterGenerator( insertStart, 1l ).build();

        operationGenerator = generatorBuilder.discreteGenerator( operations ).build();

        transactionInsertKeyGenerator = generatorBuilder.counterGenerator( recordCount, 1l ).withMinMaxLast(
                recordCount, recordCount ).build();

        requestKeyGenerator = generatorBuilder.dynamicRangeUniformNumberGenerator( transactionInsertKeyGenerator ).build();

    }

    @Override
    public boolean doInsert( DB db, Object threadstate ) throws WorkloadException
    {
        return WorkloadOperation2.doInsert( db, loadInsertKeyGenerator, insertFieldSelectionGenerator,
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
     * 
     * @throws DBException
     */
    @Override
    public boolean doTransaction( DB db, Object threadstate ) throws WorkloadException
    {
        String op = operationGenerator.next();

        if ( op.equals( "INSERT" ) )
        {
            return WorkloadOperation2.doInsert( db, transactionInsertKeyGenerator, insertFieldSelectionGenerator,
                    fieldValuelengthGenerator, IS_ORDERED_INSERTS, TABLE );
        }
        else if ( op.equals( "READ" ) )
        {
            return WorkloadOperation2.doRead( db, requestKeyGenerator, readFieldSelectionGenerator, IS_ORDERED_INSERTS,
                    TABLE );
        }
        else if ( op.equals( "UPDATE" ) )
        {
            return WorkloadOperation2.doUpdate( db, requestKeyGenerator, fieldValuelengthGenerator,
                    readFieldSelectionGenerator, IS_ORDERED_INSERTS, TABLE );
        }
        else if ( op.equals( "SCAN" ) )
        {
            return WorkloadOperation2.doScan( db, requestKeyGenerator, scanLengthGenerator,
                    readFieldSelectionGenerator, IS_ORDERED_INSERTS, TABLE );
        }
        else if ( op.equals( "READMODIFYWRITE" ) )
        {
            return WorkloadOperation2.doReadModifyWrite( db, requestKeyGenerator, readFieldSelectionGenerator,
                    fieldValuelengthGenerator, IS_ORDERED_INSERTS, TABLE );
        }
        else
        {
            return false;
        }
    }
}
