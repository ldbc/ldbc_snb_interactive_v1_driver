package com.ldbc.driver.db.basic;

import java.util.Map;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbException;
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
}
