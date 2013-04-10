package com.yahoo.ycsb.workloads;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBKey;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.measurements.Measurements;

public abstract class WorkloadOperation
{
    private final static String FIELD_NAME_PREFIX = "field";
    private final static String KEY_NAME_PREFIX = "user";

    public static boolean doRead( DB db, Generator<Long> keyGenerator,
            Generator<Long> transactionInsertKeySequenceGenerator, boolean orderedInserts, boolean readAllFields,
            Generator<Long> fieldNameGenerator, String table ) throws WorkloadException
    {
        // choose a random key
        DBKey key = WorkloadUtils.nextKey( keyGenerator, transactionInsertKeySequenceGenerator );

        boolean hashKeyNumber = !orderedInserts;
        String keyName = ( hashKeyNumber ) ? key.getHashed() : key.getPrefixed( KEY_NAME_PREFIX );

        HashSet<String> fields = null;

        if ( !readAllFields )
        {
            // read a random field
            String fieldname = FIELD_NAME_PREFIX + fieldNameGenerator.next();

            fields = new HashSet<String>();
            fields.add( fieldname );
        }

        if ( db.read( table, keyName, fields, new HashMap<String, ByteIterator>() ) == 0 )
            return true;
        else
            return false;
    }

    public static boolean doReadModifyWrite( DB db, Generator<Long> keyChooser,
            Generator<Long> transactionInsertKeySequence, boolean orderedInserts, boolean readAllFields,
            boolean writeAllFields, Generator<Long> fieldChooser, String table, int fieldCount,
            Generator<Long> fieldLengthGenerator ) throws WorkloadException
    {
        // choose a random key
        DBKey key = WorkloadUtils.nextKey( keyChooser, transactionInsertKeySequence );

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
            values = WorkloadUtils.buildAllValues( fieldCount, fieldLengthGenerator, FIELD_NAME_PREFIX );
        }
        else
        {
            // update one random field
            values = WorkloadUtils.buildOneValue( fieldChooser, fieldLengthGenerator, KEY_NAME_PREFIX );
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
        DBKey startKey = WorkloadUtils.nextKey( keyGenerator, transactionInsertKeySequenceGenerator );

        boolean hashKeyNumber = !orderedInserts;
        String startKeyName = ( hashKeyNumber ) ? startKey.getHashed() : startKey.getPrefixed( KEY_NAME_PREFIX );

        // choose random scan length
        // TODO should be a long, but DB class API would need to change
        // TODO casting hack for now, but should work due to generator range
        int scanLength = (int) ( (long) scanLengthGenerator.next() );

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
            int fieldCount, Generator<Long> fieldLengthGenerator, Generator<Long> fieldNameGenerator, String table )
            throws WorkloadException
    {
        // choose a random key
        DBKey key = WorkloadUtils.nextKey( keyGenerator, transactionInsertKeySequenceGenerator );

        boolean hashKeyNumber = !orderedInserts;
        String keyName = ( hashKeyNumber ) ? key.getHashed() : key.getPrefixed( KEY_NAME_PREFIX );

        HashMap<String, ByteIterator> values;

        if ( writeAllFields )
        {
            // update all fields
            values = WorkloadUtils.buildAllValues( fieldCount, fieldLengthGenerator, FIELD_NAME_PREFIX );
        }
        else
        {
            // update one random field
            values = WorkloadUtils.buildOneValue( fieldNameGenerator, fieldLengthGenerator, KEY_NAME_PREFIX );
        }

        if ( db.update( table, keyName, values ) == 0 )
            return true;
        else
            return false;
    }

    public static boolean doInsert( DB db, Generator<Long> keyGenerator, boolean orderedInserts, int fieldCount,
            Generator<Long> fieldLengthGenerator, String table ) throws WorkloadException
    {
        DBKey key = new DBKey( keyGenerator.next() );

        boolean hashKeyNumber = !orderedInserts;
        String keyName = ( hashKeyNumber ) ? key.getHashed() : key.getPrefixed( KEY_NAME_PREFIX );

        HashMap<String, ByteIterator> values = WorkloadUtils.buildAllValues( fieldCount, fieldLengthGenerator,
                FIELD_NAME_PREFIX );

        if ( db.insert( table, keyName, values ) == 0 )
            return true;
        else
            return false;
    }
}
