package com.ldbc.driver.testutils;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResultReport;

public class NothingOperationHandler extends OperationHandler<NothingOperation> {
    @Override
    protected OperationResultReport executeOperation(NothingOperation operation) throws DbException {
        return null;
    }
}
