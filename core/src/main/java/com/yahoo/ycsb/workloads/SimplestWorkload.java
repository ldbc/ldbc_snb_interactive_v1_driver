package com.yahoo.ycsb.workloads;

import java.util.ArrayList;
import java.util.Map;

import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.GeneratorBuilder;
import com.yahoo.ycsb.util.Pair;
import com.yahoo.ycsb.util.Utils;

public class SimplestWorkload extends Workload
{
    public static String table;

    int fieldCount;
    boolean readAllFields;
    boolean writeAllFields;
    boolean orderedInserts;
    long recordCount;

    Generator<Long> keySequenceGenerator;
    Generator<Long> fieldLengthGenerator;
    Generator<Long> keyGenerator;
    Generator<Long> fieldNameGenerator;
    Generator<Long> scanLengthGenerator;
    Generator<Long> transactionInsertKeySequenceGenerator;
    Generator<Object> operationGenerator;

    /**
     * Called once, in main client thread, before operations are started
     */
    @Override
    public void init( Map<String, String> properties, GeneratorBuilder generatorBuilder ) throws WorkloadException
    {
        super.init( properties, generatorBuilder );
        recordCount = Integer.parseInt( properties.get( Client.RECORD_COUNT ) );
        table = "usertable";
        // number of fields in a record
        fieldCount = 10;

        // field key name
        fieldNameGenerator = generatorBuilder.newUniformNumberGenerator( (long) 0, (long) ( fieldCount - 1 ) ).build();
        // field value size: length in bytes
        fieldLengthGenerator = generatorBuilder.newUniformNumberGenerator( 1l, 100l ).build();

        // proportion of transactions reads/update/insert/scan/read-modify-write
        ArrayList<Pair<Double, Object>> operations = new ArrayList<Pair<Double, Object>>();
        operations.add( new Pair<Double, Object>( 0.95, "READ" ) );
        operations.add( new Pair<Double, Object>( 0.05, "UPDATE" ) );
        operations.add( new Pair<Double, Object>( 0.00, "INSERT" ) );
        operations.add( new Pair<Double, Object>( 0.00, "SCAN" ) );
        operations.add( new Pair<Double, Object>( 0.00, "READMODIFYWRITE" ) );

        // distribution of requests across keyspace
        keyGenerator = generatorBuilder.newUniformNumberGenerator( 0l, ( recordCount - 1 ) ).build();

        // max scan length (number of records)
        long maxScanlength = 1000;
        scanLengthGenerator = generatorBuilder.newUniformNumberGenerator( 1l, maxScanlength ).build();

        // read one field (false) or all fields (true) of a record
        readAllFields = true;
        // write one field (false) or all fields (true) of a record
        writeAllFields = false;

        // order to insert records: "ordered" (true), "hashed" (false)
        orderedInserts = false;

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
        long insertStart = Long.parseLong( Utils.mapGetDefault( properties, Workload.INSERT_START,
                Workload.INSERT_START_DEFAULT ) );
        keySequenceGenerator = generatorBuilder.newCounterGenerator( insertStart, 1l ).build();

        operationGenerator = generatorBuilder.newDiscreteGenerator( operations ).build();

        transactionInsertKeySequenceGenerator = generatorBuilder.newCounterGenerator( recordCount, 1l ).build();
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
        return WorkloadOperation.doInsert( db, keySequenceGenerator, orderedInserts, fieldCount, fieldLengthGenerator,
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
     * 
     * @throws DBException
     */
    @Override
    public boolean doTransaction( DB db, Object threadstate ) throws WorkloadException
    {
        String op = (String) operationGenerator.next();

        if ( op.equals( "INSERT" ) )
        {
            return WorkloadOperation.doInsert( db, transactionInsertKeySequenceGenerator, orderedInserts, fieldCount,
                    fieldLengthGenerator, table );
        }
        else if ( op.equals( "READ" ) )
        {
            return WorkloadOperation.doRead( db, keyGenerator, transactionInsertKeySequenceGenerator, orderedInserts,
                    readAllFields, fieldNameGenerator, table );
        }
        else if ( op.equals( "UPDATE" ) )
        {
            return WorkloadOperation.doUpdate( db, keyGenerator, transactionInsertKeySequenceGenerator, orderedInserts,
                    writeAllFields, fieldCount, fieldLengthGenerator, fieldNameGenerator, table );
        }
        else if ( op.equals( "SCAN" ) )
        {
            return WorkloadOperation.doScan( db, keyGenerator, transactionInsertKeySequenceGenerator, orderedInserts,
                    scanLengthGenerator, readAllFields, fieldNameGenerator, table );
        }
        else if ( op.equals( "READMODIFYWRITE" ) )
        {
            return WorkloadOperation.doReadModifyWrite( db, keyGenerator, transactionInsertKeySequenceGenerator,
                    orderedInserts, readAllFields, writeAllFields, fieldNameGenerator, table, fieldCount,
                    fieldLengthGenerator );
        }
        else
        {
            return false;
        }
    }
}
