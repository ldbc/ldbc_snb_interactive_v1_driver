package com.yahoo.ycsb.workloads;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.IntegerGenerator;
import com.yahoo.ycsb.measurements.Measurements;

public abstract class WorkloadOperation
{
    public static boolean doRead( DB db, IntegerGenerator keyChooser, CounterGenerator transactionInsertKeySequence,
            boolean orderedInserts, boolean readAllFields, Generator fieldChooser, String table )
    {
        // choose a random key
        int keynum = WorkloadUtils.nextKeynum( keyChooser, transactionInsertKeySequence );

        String keyname = WorkloadUtils.buildKeyName( keynum, orderedInserts );

        HashSet<String> fields = null;

        if ( !readAllFields )
        {
            // read a random field
            String fieldname = "field" + fieldChooser.nextString();

            fields = new HashSet<String>();
            fields.add( fieldname );
        }

        if ( db.read( table, keyname, fields, new HashMap<String, ByteIterator>() ) == 0 )
            return true;
        else
            return false;
    }

    public static boolean doReadModifyWrite( DB db, IntegerGenerator keyChooser,
            CounterGenerator transactionInsertKeySequence, boolean orderedInserts, boolean readAllFields,
            boolean writeAllFields, Generator fieldChooser, String table, int fieldCount,
            IntegerGenerator fieldLengthGenerator )
    {
        // choose a random key
        int keynum = WorkloadUtils.nextKeynum( keyChooser, transactionInsertKeySequence );

        String keyname = WorkloadUtils.buildKeyName( keynum, orderedInserts );

        HashSet<String> fields = null;

        if ( !readAllFields )
        {
            // read a random field
            String fieldname = "field" + fieldChooser.nextString();

            fields = new HashSet<String>();
            fields.add( fieldname );
        }

        HashMap<String, ByteIterator> values;

        if ( writeAllFields )
        {
            // update all fields
            values = WorkloadUtils.buildValues( fieldCount, fieldLengthGenerator );
        }
        else
        {
            // update one random field
            values = WorkloadUtils.buildUpdate( fieldChooser, fieldLengthGenerator );
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

    public static boolean doScan( DB db, IntegerGenerator keyChooser, CounterGenerator transactionInsertKeySequence,
            boolean orderedInserts, IntegerGenerator scanLength, boolean readAllFields, Generator fieldChooser,
            String table )
    {
        // choose a random key
        int keynum = WorkloadUtils.nextKeynum( keyChooser, transactionInsertKeySequence );

        String startkeyname = WorkloadUtils.buildKeyName( keynum, orderedInserts );

        // choose a random scan length
        int len = scanLength.nextInt();

        HashSet<String> fields = null;

        if ( !readAllFields )
        {
            // read a random field
            String fieldname = "field" + fieldChooser.nextString();

            fields = new HashSet<String>();
            fields.add( fieldname );
        }

        if ( db.scan( table, startkeyname, len, fields, new Vector<HashMap<String, ByteIterator>>() ) == 0 )
            return true;
        else
            return false;
    }

    public static boolean doUpdate( DB db, IntegerGenerator keyChooser, CounterGenerator transactionInsertKeySequence,
            boolean orderedInserts, boolean writeAllFields, int fieldCount, IntegerGenerator fieldLengthGenerator,
            Generator fieldChooser, String table )
    {
        // choose a random key
        int keynum = WorkloadUtils.nextKeynum( keyChooser, transactionInsertKeySequence );

        String keyname = WorkloadUtils.buildKeyName( keynum, orderedInserts );

        HashMap<String, ByteIterator> values;

        if ( writeAllFields )
        {
            // update all fields
            values = WorkloadUtils.buildValues( fieldCount, fieldLengthGenerator );
        }
        else
        {
            // update one random field
            values = WorkloadUtils.buildUpdate( fieldChooser, fieldLengthGenerator );
        }

        if ( db.update( table, keyname, values ) == 0 )
            return true;
        else
            return false;
    }

    public static boolean doInsert( DB db, CounterGenerator transactionInsertKeySequence, boolean orderedInserts,
            int fieldCount, IntegerGenerator fieldLengthGenerator, String table )
    {
        // choose the next key
        int keynum = transactionInsertKeySequence.nextInt();

        String dbkey = WorkloadUtils.buildKeyName( keynum, orderedInserts );

        HashMap<String, ByteIterator> values = WorkloadUtils.buildValues( fieldCount, fieldLengthGenerator );

        if ( db.insert( table, dbkey, values ) == 0 )
            return true;
        else
            return false;
    }

}
