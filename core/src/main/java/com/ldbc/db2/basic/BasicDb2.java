package com.ldbc.db2.basic;

import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.ldbc.data.ByteIterator;
import com.ldbc.db2.Db2;
import com.ldbc.db2.DbException2;
import com.ldbc.db2.OperationHandler2;
import com.ldbc.db2.OperationResult2;
import com.ldbc.workloads2.simple.InsertOperation2;
import com.ldbc.workloads2.simple.ReadModifyWriteOperation2;
import com.ldbc.workloads2.simple.ReadOperation2;
import com.ldbc.workloads2.simple.ScanOperation2;
import com.ldbc.workloads2.simple.UpdateOperation2;

public class BasicDb2 extends Db2
{
    @Override
    public void init( Map<String, String> properties ) throws DbException2
    {
        registerOperationHandler( InsertOperation2.class, new InsertOperationHandler2() );
        registerOperationHandler( ReadOperation2.class, new ReadOperationHandler2() );
        registerOperationHandler( UpdateOperation2.class, new UpdateOperationHandler2() );
        registerOperationHandler( ScanOperation2.class, new ScanOperationHandler2() );
        registerOperationHandler( ReadModifyWriteOperation2.class, new ReadModifyWriteOperationHandler2() );
    }

    @Override
    public void cleanup() throws DbException2
    {
    }
}

class InsertOperationHandler2 extends OperationHandler2<InsertOperation2>
{
    @Override
    public OperationResult2 executeOperation( InsertOperation2 operation )
    {
        String table = operation.getTable();
        String keyName = operation.getKey();
        Map<String, ByteIterator> valuedFields = operation.getValuedFields();

        // TODO do things

        int resultCode = -1;
        Object result = null;

        return operation.buildResult( resultCode, result );
    }
}

class ReadOperationHandler2 extends OperationHandler2<ReadOperation2>
{
    @Override
    public OperationResult2 executeOperation( ReadOperation2 operation )
    {
        String table = operation.getTable();
        String keyName = operation.getKey();
        Set<String> fields = operation.getFields();

        // TODO do things

        int resultCode = -1;
        Map<String, ByteIterator> result = null;

        return operation.buildResult( resultCode, result );
    }
}

class UpdateOperationHandler2 extends OperationHandler2<UpdateOperation2>
{
    @Override
    public OperationResult2 executeOperation( UpdateOperation2 operation )
    {
        String table = operation.getTable();
        String key = operation.getKey();
        Map<String, ByteIterator> values = operation.getValues();

        // TODO do things

        int resultCode = -1;
        Object result = null;

        return operation.buildResult( resultCode, result );
    }
}

class ScanOperationHandler2 extends OperationHandler2<ScanOperation2>
{
    @Override
    public OperationResult2 executeOperation( ScanOperation2 operation )
    {
        String table = operation.getTable();
        String startkey = operation.getStartkey();
        int recordcount = operation.getRecordcount();
        Set<String> fields = operation.getFields();

        // TODO do things

        int resultCode = -1;
        Vector<Map<String, ByteIterator>> result = null;

        return operation.buildResult( resultCode, result );
    }
}

class ReadModifyWriteOperationHandler2 extends OperationHandler2<ReadModifyWriteOperation2>
{
    @Override
    public OperationResult2 executeOperation( ReadModifyWriteOperation2 operation )
    {
        String table = operation.getTable();
        String key = operation.getKey();
        Set<String> fields = operation.getFields();
        Map<String, ByteIterator> values = operation.getValues();

        // TODO do things

        int resultCode = -1;
        Object result = null;

        return operation.buildResult( resultCode, result );
    }
}
