package com.ldbc.driver.workloads.dummy;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;

public class NothingOperationHandler implements OperationHandler<NothingOperation, DbConnectionState> {
    @Override
    public void executeOperation(NothingOperation operation, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
    }
}
