package com.yahoo.ycsb.workloads;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.measurements.Measurements;

public abstract class WorkloadOperation
{
    private final static String FIELD_NAME_PREFIX = "field";
    private final static String KEY_NAME_PREFIX = "user";

    public static boolean doRead( DB db, Generator keyChooser, CounterGenerator transactionInsertKeySequence,
            boolean orderedInserts, boolean readAllFields, Generator fieldChooser, String table )
            throws WorkloadException
    {
        // choose a random key
        long keynum = WorkloadUtils.nextKeyNumber( keyChooser, transactionInsertKeySequence );

        String keyname = WorkloadUtils.keyNumberToKeyName( keynum, !orderedInserts, KEY_NAME_PREFIX );

        HashSet<String> fields = null;

        if ( !readAllFields )
        {
            // read a random field
            String fieldname = FIELD_NAME_PREFIX + fieldChooser.next();

            fields = new HashSet<String>();
            fields.add( fieldname );
        }

        if ( db.read( table, keyname, fields, new HashMap<String, ByteIterator>() ) == 0 )
            return true;
        else
            return false;
    }

    public static boolean doReadModifyWrite( DB db, Generator<Integer> keyChooser,
            CounterGenerator transactionInsertKeySequence, boolean orderedInserts, boolean readAllFields,
            boolean writeAllFields, Generator<Integer> fieldChooser, String table, int fieldCount,
            Generator<Integer> fieldLengthGenerator ) throws WorkloadException
    {
        // choose a random key
        long keynum = WorkloadUtils.nextKeyNumber( keyChooser, transactionInsertKeySequence );

        String keyname = WorkloadUtils.keyNumberToKeyName( keynum, !orderedInserts, KEY_NAME_PREFIX );

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

        int result = db.read( table, keyname, fields, new HashMap<String, ByteIterator>() );

        result += db.update( table, keyname, values );

        long en = System.nanoTime();

        Measurements.getMeasurements().measure( "READ-MODIFY-WRITE", (int) ( ( en - st ) / 1000 ) );

        if ( result == 0 )
            return true;
        else
            return false;

    }

    public static boolean doScan( DB db, Generator<Integer> keyChooser, CounterGenerator transactionInsertKeySequence,
            boolean orderedInserts, Generator<Integer> scanLength, boolean readAllFields, Generator fieldChooser,
            String table ) throws WorkloadException
    {
        // choose a random key
        long keynum = WorkloadUtils.nextKeyNumber( keyChooser, transactionInsertKeySequence );

        String startkeyname = WorkloadUtils.keyNumberToKeyName( keynum, !orderedInserts, KEY_NAME_PREFIX );

        // choose a random scan length
        int len = scanLength.next();

        HashSet<String> fields = null;

        if ( !readAllFields )
        {
            // read a random field
            String fieldname = FIELD_NAME_PREFIX + fieldChooser.next();

            fields = new HashSet<String>();
            fields.add( fieldname );
        }

        if ( db.scan( table, startkeyname, len, fields, new Vector<HashMap<String, ByteIterator>>() ) == 0 )
            return true;
        else
            return false;
    }

    public static boolean doUpdate( DB db, Generator<Integer> keyChooser,
            CounterGenerator transactionInsertKeySequence, boolean orderedInserts, boolean writeAllFields,
            int fieldCount, Generator<Integer> fieldLengthGenerator, Generator<Integer> fieldChooser, String table )
            throws WorkloadException
    {
        // choose a random key
        long keynum = WorkloadUtils.nextKeyNumber( keyChooser, transactionInsertKeySequence );

        String keyname = WorkloadUtils.keyNumberToKeyName( keynum, !orderedInserts, KEY_NAME_PREFIX );

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

        if ( db.update( table, keyname, values ) == 0 )
            return true;
        else
            return false;
    }

    public static boolean doInsert( DB db, Generator<Integer> keySequence, boolean orderedInserts, int fieldCount,
            Generator<Integer> fieldLengthGenerator, String table ) throws WorkloadException
    {
        // choose the next key
        long keynum = keySequence.next();

        String dbkey = WorkloadUtils.keyNumberToKeyName( keynum, !orderedInserts, KEY_NAME_PREFIX );

        HashMap<String, ByteIterator> values = WorkloadUtils.buildAllValues( fieldCount, fieldLengthGenerator,
                FIELD_NAME_PREFIX );

        if ( db.insert( table, dbkey, values ) == 0 )
            return true;
        else
            return false;
    }
}
