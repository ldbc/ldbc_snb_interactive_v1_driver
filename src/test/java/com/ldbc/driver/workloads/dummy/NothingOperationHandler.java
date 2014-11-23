package com.ldbc.driver.workloads.dummy;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResultReport;

public class NothingOperationHandler extends OperationHandler<NothingOperation, DbConnectionState> {
    @Override
    public OperationResultReport executeOperation(NothingOperation operation, DbConnectionState dbConnectionState) throws DbException {
        return null;
    }
}
