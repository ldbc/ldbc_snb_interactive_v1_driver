package com.yahoo.ycsb.workloads;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBRecordKey;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.GeneratorBuilder;
import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.util.ByteIterator;
import com.yahoo.ycsb.util.Pair;

public abstract class WorkloadOperation
{
    private final static String FIELD_NAME_PREFIX = "field";
    private final static String KEY_NAME_PREFIX = "user";

    // TODO migrate doRead() to using implementation, once it's done
    public static boolean doReadNEW( DB db, Generator<Long> requestKeyGenerator,
            Generator<Long> transactionInsertKeySequenceGenerator, boolean orderedInserts, boolean readAllFields,
            Generator<Long> fieldChooserGenerator, String table, GeneratorBuilder generatorBuilder )
            throws WorkloadException
    {
        // choose a random key
        DBRecordKey key = WorkloadUtils.nextKey( requestKeyGenerator, transactionInsertKeySequenceGenerator );

        boolean hashKeyNumber = !orderedInserts;
        String keyName = ( hashKeyNumber ) ? key.getHashed() : key.getPrefixed( KEY_NAME_PREFIX );

        Pair<Double, String> item1 = new Pair<Double, String>( 1.0, FIELD_NAME_PREFIX + 1l );
        Pair<Double, String> item2 = new Pair<Double, String>( 2.0, FIELD_NAME_PREFIX + 2l );
        Pair<Double, String> item3 = new Pair<Double, String>( 4.0, FIELD_NAME_PREFIX + 3l );
        ArrayList<Pair<Double, String>> items = new ArrayList<Pair<Double, String>>();
        items.add( item1 );
        items.add( item2 );
        items.add( item3 );
        Generator<Set<String>> fieldNamesGenerator = generatorBuilder.discreteMultiGenerator( items, 1 ).build();

        Set<String> fields = fieldNamesGenerator.next();

        if ( !readAllFields )
        {
            // read a random field
            String fieldname = FIELD_NAME_PREFIX + fieldChooserGenerator.next();

            fields = new HashSet<String>();
            fields.add( fieldname );
        }

        if ( db.read( table, keyName, fields, new HashMap<String, ByteIterator>() ) == 0 )
            return true;
        else
            return false;
    }

    public static boolean doRead( DB db, Generator<Long> requestKeyGenerator,
            Generator<Long> transactionInsertKeySequenceGenerator, boolean orderedInserts, boolean readAllFields,
            Generator<Long> fieldChooserGenerator, String table ) throws WorkloadException
    {
        // choose a random key
        DBRecordKey key = WorkloadUtils.nextKey( requestKeyGenerator, transactionInsertKeySequenceGenerator );

        boolean hashKeyNumber = !orderedInserts;
        String keyName = ( hashKeyNumber ) ? key.getHashed() : key.getPrefixed( KEY_NAME_PREFIX );

        HashSet<String> fields = null;

        if ( !readAllFields )
        {
            // read a random field
            String fieldname = FIELD_NAME_PREFIX + fieldChooserGenerator.next();

            fields = new HashSet<String>();
            fields.add( fieldname );
        }

        if ( db.read( table, keyName, fields, new HashMap<String, ByteIterator>() ) == 0 )
            return true;
        else
            return false;
    }

    public static boolean doReadModifyWrite( DB db, Generator<Long> keyGenerator,
            Generator<Long> transactionInsertKeySequenceGenerator, boolean orderedInserts, boolean readAllFields,
            boolean writeAllFields, Generator<Long> fieldChooser, String table, long fieldCount,
            Generator<Long> fieldLengthGenerator ) throws WorkloadException
    {
        // choose a random key
        DBRecordKey key = WorkloadUtils.nextKey( keyGenerator, transactionInsertKeySequenceGenerator );

        boolean hashKeyNumber = !orderedInserts;
        String keyName = ( hashKeyNumber ) ? key.getHashed() : key.getPrefixed( KEY_NAME_PREFIX );

        HashSet<String> fields = null;

        if ( !readAllFields )
        {
            // read a random field
            String fieldname = FIELD_NAME_PREFIX + fieldChooser.next();

            fields = new HashSet<String>();
            fields.add( fieldname );
        }

        HashMap<String, ByteIterator> values;

        if ( writeAllFields )
        {
            // update all fields
            values = WorkloadUtils.buildRecordWithAllFields( fieldCount, fieldLengthGenerator, FIELD_NAME_PREFIX );
        }
        else
        {
            // update one random field
            values = WorkloadUtils.buildRecordWithOneField( fieldChooser, fieldLengthGenerator, KEY_NAME_PREFIX );
        }

        // do the transaction

        long st = System.nanoTime();

        int result = db.read( table, keyName, fields, new HashMap<String, ByteIterator>() );

        result += db.update( table, keyName, values );

        long en = System.nanoTime();

        Measurements.getMeasurements().measure( "READ-MODIFY-WRITE", (int) ( ( en - st ) / 1000 ) );

        if ( result == 0 )
            return true;
        else
            return false;

    }

    public static boolean doScan( DB db, Generator<Long> keyGenerator,
            Generator<Long> transactionInsertKeySequenceGenerator, boolean orderedInserts,
            Generator<Long> scanLengthGenerator, boolean readAllFields, Generator<Long> fieldNameGenerator, String table )
            throws WorkloadException
    {
        // choose a random key
        DBRecordKey startKey = WorkloadUtils.nextKey( keyGenerator, transactionInsertKeySequenceGenerator );

        boolean hashKeyNumber = !orderedInserts;
        String startKeyName = ( hashKeyNumber ) ? startKey.getHashed() : startKey.getPrefixed( KEY_NAME_PREFIX );

        // choose random scan length
        // TODO should be a long, but DB class API would need to change
        int scanLength = scanLengthGenerator.next().intValue();

        HashSet<String> fields = null;

        if ( !readAllFields )
        {
            // read a random field
            String fieldname = FIELD_NAME_PREFIX + fieldNameGenerator.next();

            fields = new HashSet<String>();
            fields.add( fieldname );
        }

        if ( db.scan( table, startKeyName, scanLength, fields, new Vector<HashMap<String, ByteIterator>>() ) == 0 )
            return true;
        else
            return false;
    }

    public static boolean doUpdate( DB db, Generator<Long> keyGenerator,
            Generator<Long> transactionInsertKeySequenceGenerator, boolean orderedInserts, boolean writeAllFields,
            long fieldCount, Generator<Long> fieldLengthGenerator, Generator<Long> fieldNameGenerator, String table )
            throws WorkloadException
    {
        // choose a random key
        DBRecordKey key = WorkloadUtils.nextKey( keyGenerator, transactionInsertKeySequenceGenerator );

        boolean hashKeyNumber = !orderedInserts;
        String keyName = ( hashKeyNumber ) ? key.getHashed() : key.getPrefixed( KEY_NAME_PREFIX );

        HashMap<String, ByteIterator> values;

        if ( writeAllFields )
        {
            // update all fields
            values = WorkloadUtils.buildRecordWithAllFields( fieldCount, fieldLengthGenerator, FIELD_NAME_PREFIX );
        }
        else
        {
            // update one random field
            values = WorkloadUtils.buildRecordWithOneField( fieldNameGenerator, fieldLengthGenerator, KEY_NAME_PREFIX );
        }

        if ( db.update( table, keyName, values ) == 0 )
            return true;
        else
            return false;
    }

    public static boolean doInsert( DB db, Generator<Long> keyGenerator, boolean orderedInserts, long fieldCount,
            Generator<Long> fieldLengthGenerator, String table ) throws WorkloadException
    {
        DBRecordKey key = new DBRecordKey( keyGenerator.next() );

        boolean hashKeyNumber = !orderedInserts;
        String keyName = ( hashKeyNumber ) ? key.getHashed() : key.getPrefixed( KEY_NAME_PREFIX );

        HashMap<String, ByteIterator> values = WorkloadUtils.buildRecordWithAllFields( fieldCount,
                fieldLengthGenerator, FIELD_NAME_PREFIX );

        if ( db.insert( table, keyName, values ) == 0 )
            return true;
        else
            return false;
    }
}
