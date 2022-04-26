package org.ldbcouncil.driver.workloads.dummy;

import org.ldbcouncil.driver.DbConnectionState;
import org.ldbcouncil.driver.DbException;
import org.ldbcouncil.driver.OperationHandler;
import org.ldbcouncil.driver.ResultReporter;

public class NothingOperationHandler implements OperationHandler<NothingOperation, DbConnectionState> {
    @Override
    public void executeOperation(NothingOperation operation, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
    }
}
