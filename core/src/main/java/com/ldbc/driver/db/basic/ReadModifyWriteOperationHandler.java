package com.ldbc.driver.db.basic;

import java.util.Map;
import java.util.Set;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.data.ByteIterator;
import com.ldbc.driver.workloads.simple.ReadModifyWriteOperation;

public class ReadModifyWriteOperationHandler extends OperationHandler<ReadModifyWriteOperation>
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
