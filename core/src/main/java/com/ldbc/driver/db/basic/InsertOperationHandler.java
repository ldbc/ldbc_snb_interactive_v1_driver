package com.ldbc.driver.db.basic;

import java.util.Map;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.data.ByteIterator;
import com.ldbc.driver.workloads.simple.InsertOperation;

public class InsertOperationHandler extends OperationHandler<InsertOperation>
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
