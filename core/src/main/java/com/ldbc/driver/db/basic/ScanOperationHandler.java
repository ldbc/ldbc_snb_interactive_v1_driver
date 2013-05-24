package com.ldbc.driver.db.basic;

import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.data.ByteIterator;
import com.ldbc.driver.workloads.simple.ScanOperation;

public class ScanOperationHandler extends OperationHandler<ScanOperation>
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
