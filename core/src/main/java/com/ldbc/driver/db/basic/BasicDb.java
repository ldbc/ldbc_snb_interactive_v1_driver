package com.ldbc.driver.db.basic;

import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.data.ByteIterator;
import com.ldbc.driver.workloads.simple.InsertOperation;
import com.ldbc.driver.workloads.simple.ReadModifyWriteOperation;
import com.ldbc.driver.workloads.simple.ReadOperation;
import com.ldbc.driver.workloads.simple.ScanOperation;
import com.ldbc.driver.workloads.simple.UpdateOperation;

public class BasicDb extends Db
{
    @Override
    public void onInit( Map<String, String> properties ) throws DbException
    {
        registerOperationHandler( InsertOperation.class, InsertOperationHandler2.class );
        registerOperationHandler( ReadOperation.class, ReadOperationHandler2.class );
        registerOperationHandler( UpdateOperation.class, UpdateOperationHandler2.class );
        registerOperationHandler( ScanOperation.class, ScanOperationHandler2.class );
        registerOperationHandler( ReadModifyWriteOperation.class, ReadModifyWriteOperationHandler2.class );
    }

    @Override
    public void onCleanup() throws DbException
    {
    }
}

class InsertOperationHandler2 extends OperationHandler<InsertOperation>
{
    @Override
    public OperationResult executeOperation( InsertOperation operation )
    {
        String table = operation.getTable();
        String keyName = operation.getKey();
        Map<String, ByteIterator> valuedFields = operation.getValuedFields();

        // TODO do things

        int resultCode = 0;
        Object result = null;

        return operation.buildResult( resultCode, result );
    }
}

class ReadOperationHandler2 extends OperationHandler<ReadOperation>
{
    @Override
    public OperationResult executeOperation( ReadOperation operation )
    {
        String table = operation.getTable();
        String keyName = operation.getKey();
        Set<String> fields = operation.getFields();

        // TODO do things

        int resultCode = 0;
        Map<String, ByteIterator> result = null;

        return operation.buildResult( resultCode, result );
    }
}

class UpdateOperationHandler2 extends OperationHandler<UpdateOperation>
{
    @Override
    public OperationResult executeOperation( UpdateOperation operation )
    {
        String table = operation.getTable();
        String key = operation.getKey();
        Map<String, ByteIterator> values = operation.getValues();

        // TODO do things

        int resultCode = 0;
        Object result = null;

        return operation.buildResult( resultCode, result );
    }
}

class ScanOperationHandler2 extends OperationHandler<ScanOperation>
{
    @Override
    public OperationResult executeOperation( ScanOperation operation )
    {
        String table = operation.getTable();
        String startkey = operation.getStartkey();
        int recordcount = operation.getRecordcount();
        Set<String> fields = operation.getFields();

        // TODO do things

        int resultCode = 0;
        Vector<Map<String, ByteIterator>> result = null;

        return operation.buildResult( resultCode, result );
    }
}

class ReadModifyWriteOperationHandler2 extends OperationHandler<ReadModifyWriteOperation>
{
    @Override
    public OperationResult executeOperation( ReadModifyWriteOperation operation )
    {
        String table = operation.getTable();
        String key = operation.getKey();
        Set<String> fields = operation.getFields();
        Map<String, ByteIterator> values = operation.getValues();

        // TODO do things

        int resultCode = 0;
        Object result = null;

        return operation.buildResult( resultCode, result );
    }
}
