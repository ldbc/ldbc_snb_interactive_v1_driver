package com.ldbc.driver.db.basic;

import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
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
        registerOperationHandler( InsertOperation.class, InsertOperationHandler.class );
        registerOperationHandler( ReadOperation.class, ReadOperationHandler.class );
        registerOperationHandler( UpdateOperation.class, UpdateOperationHandler.class );
        registerOperationHandler( ScanOperation.class, ScanOperationHandler.class );
        registerOperationHandler( ReadModifyWriteOperation.class, ReadModifyWriteOperationHandler.class );
    }

    @Override
    public void onCleanup() throws DbException
    {
    }

    private static void sleep()
    {
        try
        {
            Thread.sleep( 100 );
        }
        catch ( InterruptedException e )
        {
        }
    }

    public static class InsertOperationHandler extends OperationHandler<InsertOperation>
    {
        @Override
        public OperationResult executeOperation( InsertOperation operation )
        {
            String table = operation.getTable();
            String keyName = operation.getKey();
            Map<String, ByteIterator> valuedFields = operation.getValuedFields();

            // TODO do things
            sleep();

            int resultCode = 0;
            Object result = null;

            return operation.buildResult( resultCode, result );
        }
    }

    public static class ReadOperationHandler extends OperationHandler<ReadOperation>
    {
        @Override
        public OperationResult executeOperation( ReadOperation operation )
        {
            String table = operation.getTable();
            String keyName = operation.getKey();
            Set<String> fields = operation.getFields();

            // TODO do things
            sleep();

            int resultCode = 0;
            Map<String, ByteIterator> result = null;

            return operation.buildResult( resultCode, result );
        }
    }

    public static class UpdateOperationHandler extends OperationHandler<UpdateOperation>
    {
        @Override
        public OperationResult executeOperation( UpdateOperation operation )
        {
            String table = operation.getTable();
            String key = operation.getKey();
            Map<String, ByteIterator> values = operation.getValues();

            // TODO do things
            sleep();

            int resultCode = 0;
            Object result = null;

            return operation.buildResult( resultCode, result );
        }
    }

    public static class ScanOperationHandler extends OperationHandler<ScanOperation>
    {
        @Override
        public OperationResult executeOperation( ScanOperation operation )
        {
            String table = operation.getTable();
            String startkey = operation.getStartkey();
            int recordcount = operation.getRecordcount();
            Set<String> fields = operation.getFields();

            // TODO do things
            sleep();

            int resultCode = 0;
            Vector<Map<String, ByteIterator>> result = null;

            return operation.buildResult( resultCode, result );
        }
    }

    public static class ReadModifyWriteOperationHandler extends OperationHandler<ReadModifyWriteOperation>
    {
        @Override
        public OperationResult executeOperation( ReadModifyWriteOperation operation )
        {
            String table = operation.getTable();
            String key = operation.getKey();
            Set<String> fields = operation.getFields();
            Map<String, ByteIterator> values = operation.getValues();

            // TODO do things
            sleep();

            int resultCode = 0;
            Object result = null;

            return operation.buildResult( resultCode, result );
        }
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException
    {
        return null;
    }

}
