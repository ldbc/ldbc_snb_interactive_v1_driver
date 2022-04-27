package org.ldbcouncil.snb.driver.workloads.dummy;

import org.ldbcouncil.snb.driver.DbConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;

public class NothingOperationHandler implements OperationHandler<NothingOperation, DbConnectionState> {
    @Override
    public void executeOperation(NothingOperation operation, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
    }
}
