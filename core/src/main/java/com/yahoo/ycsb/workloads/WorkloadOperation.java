package com.yahoo.ycsb.workloads;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBRecordKey;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.MinMaxGeneratorWrapper;
import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.util.ByteIterator;

public abstract class WorkloadOperation
{
    // TODO replace, a duplicate exists in CoreWorkload
    private final static String KEY_NAME_PREFIX = "user";

    public static boolean doInsert( DB db, Generator<Long> insertKeyGenerator,
            Generator<Set<String>> fieldSelectionGenerator, Generator<Integer> fieldValuelengthGenerator,
            boolean isOrderedInserts, String table ) throws WorkloadException
    {
        // TODO keyNameGenerator needs to be a Generator too
        // choose a random record key
        DBRecordKey key = new DBRecordKey( insertKeyGenerator.next() );
        boolean hashKeyNumber = !isOrderedInserts;
        String keyName = ( hashKeyNumber ) ? key.getHashed() : key.getPrefixed( KEY_NAME_PREFIX );

        // choose a random set of the fields to read from the record
        Map<String, ByteIterator> valuedFields = WorkloadUtils.buildValuedFields( fieldSelectionGenerator,
                fieldValuelengthGenerator );

        if ( db.insert( table, keyName, valuedFields ) == 0 )
            return true;
        else
            return false;
    }

    public static boolean doRead( DB db, Generator<Long> requestKeyGenerator,
            MinMaxGeneratorWrapper<Long> transactionInsertKeyGenerator,
            Generator<Set<String>> fieldsSelectionGenerator, boolean isOrderedInserts, String table )
            throws WorkloadException
    {
        // TODO keyNameGenerator needs to be a Generator too
        // choose a random record key
        DBRecordKey key = WorkloadUtils.nextKey( requestKeyGenerator, transactionInsertKeyGenerator );
        boolean hashKeyNumber = !isOrderedInserts;
        String keyName = ( hashKeyNumber ) ? key.getHashed() : key.getPrefixed( KEY_NAME_PREFIX );

        // choose a random set of the fields to read from the record
        Set<String> fields = fieldsSelectionGenerator.next();

        // execute read operation
        if ( db.read( table, keyName, fields, new HashMap<String, ByteIterator>() ) == 0 )
            return true;
        else
            return false;
    }

    public static boolean doUpdate( DB db, Generator<Long> requestKeyGenerator,
            MinMaxGeneratorWrapper<Long> transactionInsertKeyGenerator, Generator<Integer> fieldValuelengthGenerator,
            Generator<Set<String>> fieldSelectionGenerator, boolean isOrderedInserts, String table )
            throws WorkloadException
    {
        // TODO keyNameGenerator needs to be a Generator too
        // choose a random record key
        DBRecordKey key = WorkloadUtils.nextKey( requestKeyGenerator, transactionInsertKeyGenerator );
        boolean hashKeyNumber = !isOrderedInserts;
        String keyName = ( hashKeyNumber ) ? key.getHashed() : key.getPrefixed( KEY_NAME_PREFIX );

        // choose a random set of the fields to read from the record
        Map<String, ByteIterator> valuedFields = WorkloadUtils.buildValuedFields( fieldSelectionGenerator,
                fieldValuelengthGenerator );

        if ( db.update( table, keyName, valuedFields ) == 0 )
            return true;
        else
            return false;
    }

    public static boolean doScan( DB db, Generator<Long> requestKeyGenerator,
            MinMaxGeneratorWrapper<Long> transactionInsertKeyGenerator, Generator<Integer> scanLengthGenerator,
            Generator<Set<String>> fieldsSelectionGenerator, boolean isOrderedInserts, String table )
            throws WorkloadException
    {
        // TODO keyNameGenerator needs to be a Generator too
        // choose a random record key
        DBRecordKey startKey = WorkloadUtils.nextKey( requestKeyGenerator, transactionInsertKeyGenerator );
        boolean hashKeyNumber = !isOrderedInserts;
        String startKeyName = ( hashKeyNumber ) ? startKey.getHashed() : startKey.getPrefixed( KEY_NAME_PREFIX );

        // choose random scan length
        int scanLength = scanLengthGenerator.next();

        // choose a random set of the fields to read from the record
        Set<String> fields = fieldsSelectionGenerator.next();

        if ( db.scan( table, startKeyName, scanLength, fields, new Vector<Map<String, ByteIterator>>() ) == 0 )
            return true;
        else
            return false;
    }

    public static boolean doReadModifyWrite( DB db, Generator<Long> requestKeyGenerator,
            MinMaxGeneratorWrapper<Long> transactionInsertKeyGenerator, Generator<Set<String>> fieldSelectionGenerator,
            Generator<Integer> fieldValuelengthGenerator, boolean isOrderedInserts, String table )
            throws WorkloadException
    {
        // TODO keyNameGenerator needs to be a Generator too
        // choose a random record key
        DBRecordKey key = WorkloadUtils.nextKey( requestKeyGenerator, transactionInsertKeyGenerator );
        boolean hashKeyNumber = !isOrderedInserts;
        String keyName = ( hashKeyNumber ) ? key.getHashed() : key.getPrefixed( KEY_NAME_PREFIX );

        // choose a random set of the fields to read from the record
        Map<String, ByteIterator> valuedFields = WorkloadUtils.buildValuedFields( fieldSelectionGenerator,
                fieldValuelengthGenerator );
        Set<String> fields = valuedFields.keySet();

        // do the transaction
        long st = System.nanoTime();
        int result = db.read( table, keyName, fields, new HashMap<String, ByteIterator>() );
        result += db.update( table, keyName, valuedFields );
        long en = System.nanoTime();

        Measurements.getMeasurements().measure( "READ-MODIFY-WRITE", (int) ( ( en - st ) / 1000 ) );

        if ( result == 0 )
            return true;
        else
            return false;

    }
}
