package com.ldbc.driver.db.basic;

import java.util.Map;
import java.util.Set;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.data.ByteIterator;
import com.ldbc.driver.workloads.simple.ReadOperation;

public class ReadOperationHandler extends OperationHandler<ReadOperation>
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
