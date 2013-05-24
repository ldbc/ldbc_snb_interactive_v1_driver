package com.ldbc.driver.db.basic;

import java.util.Map;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.data.ByteIterator;
import com.ldbc.driver.workloads.simple.UpdateOperation;

public class UpdateOperationHandler extends OperationHandler<UpdateOperation>
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
